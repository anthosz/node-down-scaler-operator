package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const (
	// Annotation to identify resources that should be scaled when node is down
	enableAnnotation = "node-down-scaler/enabled"
	// Annotation to store original replica count
	originalReplicasAnnotation = "node-down-scaler/original-replicas"
	// Default resync period
	defaultResyncPeriod = time.Minute * 10
)

// Configuration options
type Config struct {
	nodeSelector string
	namespace    string
	kubeconfig   string
	masterURL    string
}

// Controller handles the scale operations
type Controller struct {
	kubeClient kubernetes.Interface
	nodeInformer cache.SharedIndexInformer
	deployInformer cache.SharedIndexInformer
	statefulSetInformer cache.SharedIndexInformer
	
	workqueue workqueue.RateLimitingInterface
	
	nodeSelector labels.Selector
	namespace    string
}

// Event represents an event to be processed
type Event struct {
	key       string
	eventType string
}

// NewController creates a new controller
func NewController(kubeClient kubernetes.Interface, factory informers.SharedInformerFactory, nodeSelector labels.Selector, namespace string) *Controller {
	
	// Create informers for nodes, deployments and statefulsets
	nodeInformer := factory.Core().V1().Nodes().Informer()
	deployInformer := factory.Apps().V1().Deployments().Informer()
	statefulSetInformer := factory.Apps().V1().StatefulSets().Informer()
	
	controller := &Controller{
		kubeClient:         kubeClient,
		nodeInformer:       nodeInformer,
		deployInformer:     deployInformer,
		statefulSetInformer: statefulSetInformer,
		workqueue:          workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "NodeDownScaler"),
		nodeSelector:       nodeSelector,
		namespace:          namespace,
	}
	
	// Add event handlers
	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueNode,
		UpdateFunc: func(old, new interface{}) {
			oldNode := old.(*corev1.Node)
			newNode := new.(*corev1.Node)
			
			// If node condition has changed, process the event
			if nodeStatusChanged(oldNode, newNode) {
				controller.enqueueNode(newNode)
			}
		},
		DeleteFunc: controller.enqueueNode,
	})
	
	// Add handlers for deployments and statefulsets
	deployInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleAddObject,
		UpdateFunc: func(old, new interface{}) {
			// We don't need to do anything special here
		},
	})
	
	statefulSetInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleAddObject,
		UpdateFunc: func(old, new interface{}) {
			// We don't need to do anything special here
		},
	})
	
	return controller
}

// nodeStatusChanged checks if a node's status has changed in a way we care about
func nodeStatusChanged(oldNode, newNode *corev1.Node) bool {
	oldReady := isNodeReady(oldNode)
	newReady := isNodeReady(newNode)
	return oldReady != newReady
}

// isNodeReady returns true if node is in Ready condition and status is True
func isNodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// handleAddObject checks if added object has our annotation
func (c *Controller) handleAddObject(obj interface{}) {
	// We don't need to do anything special for now
}

// enqueueNode adds a node to the workqueue
func (c *Controller) enqueueNode(obj interface{}) {
	var key string
	var err error
	
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	
	c.workqueue.Add(Event{
		key:       key,
		eventType: "node",
	})
}

// Run starts the controller
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()
	
	klog.Info("Starting Node Down Scaler controller")
	
	// Wait for caches to sync
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, 
		c.nodeInformer.HasSynced, 
		c.deployInformer.HasSynced, 
		c.statefulSetInformer.HasSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}
	
	// Start workers to process items from the queue
	klog.Info("Starting workers")
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}
	
	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")
	
	return nil
}

// runWorker processes items from the workqueue
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem processes a single item from the workqueue
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}
	
	// We wrap this block in a func so we can defer c.workqueue.Done
	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		
		var event Event
		var ok bool
		
		if event, ok = obj.(Event); !ok {
			c.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected Event in workqueue but got %#v", obj))
			return nil
		}
		
		// Process the event
		if err := c.processEvent(event); err != nil {
			c.workqueue.AddRateLimited(event)
			return fmt.Errorf("error syncing '%s': %s, requeuing", event.key, err.Error())
		}
		
		c.workqueue.Forget(obj)
		klog.Infof("Successfully processed event %s", event.key)
		return nil
	}(obj)
	
	if err != nil {
		runtime.HandleError(err)
		return true
	}
	
	return true
}

// processEvent handles node events
func (c *Controller) processEvent(event Event) error {
	// For node events, check if the node matches our selector
	if event.eventType == "node" {
		_, name, err := cache.SplitMetaNamespaceKey(event.key)
		if err != nil {
			return err
		}
		
		// Get the node
		nodeObj, exists, err := c.nodeInformer.GetIndexer().GetByKey(event.key)
		if err != nil {
			return err
		}
		
		// If node was deleted, we assume it's down
		var nodeDown bool
		if !exists {
			klog.Infof("Node %s was deleted, assuming it's down", name)
			nodeDown = true
		} else {
			// Check if node matches our selector
			node := nodeObj.(*corev1.Node)
			if !c.nodeSelector.Matches(labels.Set(node.Labels)) {
				// Node doesn't match our selector, ignore it
				return nil
			}
			
			nodeDown = !isNodeReady(node)
		}
		
		// Process resources based on node status
		if nodeDown {
			klog.Infof("Node %s is down, scaling down resources", name)
			return c.scaleDownResources()
		} else {
			klog.Infof("Node %s is up, scaling up resources", name)
			return c.scaleUpResources()
		}
	}
	
	return nil
}

// scaleDownResources scales down all resources with the enabled annotation
func (c *Controller) scaleDownResources() error {
	// Process deployments
	if err := c.scaleDownDeployments(); err != nil {
		return err
	}
	
	// Process statefulsets
	if err := c.scaleDownStatefulSets(); err != nil {
		return err
	}
	
	return nil
}

// scaleDownDeployments scales down deployments with the enabled annotation
func (c *Controller) scaleDownDeployments() error {
	deployments := c.deployInformer.GetIndexer().List()
	for _, obj := range deployments {
		deploy := obj.(*appsv1.Deployment)
		
		// Skip if not in our namespace (if namespace is specified)
		if c.namespace != "" && deploy.Namespace != c.namespace {
			continue
		}
		
		// Check if deployment has our annotation
		if deploy.Annotations != nil && deploy.Annotations[enableAnnotation] == "true" {
			// Skip if already scaled down
			if deploy.Annotations[originalReplicasAnnotation] != "" {
				continue
			}
			
			// Store original replicas
			originalReplicas := 0
			if deploy.Spec.Replicas != nil {
				originalReplicas = int(*deploy.Spec.Replicas)
			}
			
			if originalReplicas == 0 {
				// Already at 0, nothing to do
				continue
			}
			
			// Scale down with RetryOnConflict
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				// Get the latest version
				currentDeploy, err := c.kubeClient.AppsV1().Deployments(deploy.Namespace).Get(context.TODO(), deploy.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				
				// Add annotation with original replicas
				if currentDeploy.Annotations == nil {
					currentDeploy.Annotations = make(map[string]string)
				}
				currentDeploy.Annotations[originalReplicasAnnotation] = fmt.Sprintf("%d", originalReplicas)
				
				// Set replicas to 0
				zero := int32(0)
				currentDeploy.Spec.Replicas = &zero
				
				// Update the deployment
				_, err = c.kubeClient.AppsV1().Deployments(deploy.Namespace).Update(context.TODO(), currentDeploy, metav1.UpdateOptions{})
				return err
			})
			
			if err != nil {
				klog.Errorf("Error scaling down deployment %s/%s: %v", deploy.Namespace, deploy.Name, err)
				return err
			}
			
			klog.Infof("Scaled down deployment %s/%s from %d to 0", deploy.Namespace, deploy.Name, originalReplicas)
		}
	}
	
	return nil
}

// scaleDownStatefulSets scales down statefulsets with the enabled annotation
func (c *Controller) scaleDownStatefulSets() error {
	statefulSets := c.statefulSetInformer.GetIndexer().List()
	for _, obj := range statefulSets {
		sts := obj.(*appsv1.StatefulSet)
		
		// Skip if not in our namespace (if namespace is specified)
		if c.namespace != "" && sts.Namespace != c.namespace {
			continue
		}
		
		// Check if statefulset has our annotation
		if sts.Annotations != nil && sts.Annotations[enableAnnotation] == "true" {
			// Skip if already scaled down
			if sts.Annotations[originalReplicasAnnotation] != "" {
				continue
			}
			
			// Store original replicas
			originalReplicas := 0
			if sts.Spec.Replicas != nil {
				originalReplicas = int(*sts.Spec.Replicas)
			}
			
			if originalReplicas == 0 {
				// Already at 0, nothing to do
				continue
			}
			
			// Scale down with RetryOnConflict
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				// Get the latest version
				currentSts, err := c.kubeClient.AppsV1().StatefulSets(sts.Namespace).Get(context.TODO(), sts.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				
				// Add annotation with original replicas
				if currentSts.Annotations == nil {
					currentSts.Annotations = make(map[string]string)
				}
				currentSts.Annotations[originalReplicasAnnotation] = fmt.Sprintf("%d", originalReplicas)
				
				// Set replicas to 0
				zero := int32(0)
				currentSts.Spec.Replicas = &zero
				
				// Update the statefulset
				_, err = c.kubeClient.AppsV1().StatefulSets(sts.Namespace).Update(context.TODO(), currentSts, metav1.UpdateOptions{})
				return err
			})
			
			if err != nil {
				klog.Errorf("Error scaling down statefulset %s/%s: %v", sts.Namespace, sts.Name, err)
				return err
			}
			
			klog.Infof("Scaled down statefulset %s/%s from %d to 0", sts.Namespace, sts.Name, originalReplicas)
		}
	}
	
	return nil
}

// scaleUpResources scales up all resources with the original-replicas annotation
func (c *Controller) scaleUpResources() error {
	// Process deployments
	if err := c.scaleUpDeployments(); err != nil {
		return err
	}
	
	// Process statefulsets
	if err := c.scaleUpStatefulSets(); err != nil {
		return err
	}
	
	return nil
}

// scaleUpDeployments scales up deployments to their original replica count
func (c *Controller) scaleUpDeployments() error {
	deployments := c.deployInformer.GetIndexer().List()
	for _, obj := range deployments {
		deploy := obj.(*appsv1.Deployment)
		
		// Skip if not in our namespace (if namespace is specified)
		if c.namespace != "" && deploy.Namespace != c.namespace {
			continue
		}
		
		// Check if deployment has our annotations
		if deploy.Annotations != nil && deploy.Annotations[enableAnnotation] == "true" && deploy.Annotations[originalReplicasAnnotation] != "" {
			originalReplicasStr := deploy.Annotations[originalReplicasAnnotation]
			var originalReplicas int
			_, err := fmt.Sscanf(originalReplicasStr, "%d", &originalReplicas)
			if err != nil {
				klog.Errorf("Error parsing original replicas annotation for deployment %s/%s: %v", deploy.Namespace, deploy.Name, err)
				continue
			}
			
			// Scale up with RetryOnConflict
			err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
				// Get the latest version
				currentDeploy, err := c.kubeClient.AppsV1().Deployments(deploy.Namespace).Get(context.TODO(), deploy.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				
				// Remove original replicas annotation
				if currentDeploy.Annotations != nil {
					delete(currentDeploy.Annotations, originalReplicasAnnotation)
				}
				
				// Set replicas back to original value
				replicas := int32(originalReplicas)
				currentDeploy.Spec.Replicas = &replicas
				
				// Update the deployment
				_, err = c.kubeClient.AppsV1().Deployments(deploy.Namespace).Update(context.TODO(), currentDeploy, metav1.UpdateOptions{})
				return err
			})
			
			if err != nil {
				klog.Errorf("Error scaling up deployment %s/%s: %v", deploy.Namespace, deploy.Name, err)
				return err
			}
			
			klog.Infof("Scaled up deployment %s/%s to %d replicas", deploy.Namespace, deploy.Name, originalReplicas)
		}
	}
	
	return nil
}

// scaleUpStatefulSets scales up statefulsets to their original replica count
func (c *Controller) scaleUpStatefulSets() error {
	statefulSets := c.statefulSetInformer.GetIndexer().List()
	for _, obj := range statefulSets {
		sts := obj.(*appsv1.StatefulSet)
		
		// Skip if not in our namespace (if namespace is specified)
		if c.namespace != "" && sts.Namespace != c.namespace {
			continue
		}
		
		// Check if statefulset has our annotations
		if sts.Annotations != nil && sts.Annotations[enableAnnotation] == "true" && sts.Annotations[originalReplicasAnnotation] != "" {
			originalReplicasStr := sts.Annotations[originalReplicasAnnotation]
			var originalReplicas int
			_, err := fmt.Sscanf(originalReplicasStr, "%d", &originalReplicas)
			if err != nil {
				klog.Errorf("Error parsing original replicas annotation for statefulset %s/%s: %v", sts.Namespace, sts.Name, err)
				continue
			}
			
			// Scale up with RetryOnConflict
			err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
				// Get the latest version
				currentSts, err := c.kubeClient.AppsV1().StatefulSets(sts.Namespace).Get(context.TODO(), sts.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				
				// Remove original replicas annotation
				if currentSts.Annotations != nil {
					delete(currentSts.Annotations, originalReplicasAnnotation)
				}
				
				// Set replicas back to original value
				replicas := int32(originalReplicas)
				currentSts.Spec.Replicas = &replicas
				
				// Update the statefulset
				_, err = c.kubeClient.AppsV1().StatefulSets(sts.Namespace).Update(context.TODO(), currentSts, metav1.UpdateOptions{})
				return err
			})
			
			if err != nil {
				klog.Errorf("Error scaling up statefulset %s/%s: %v", sts.Namespace, sts.Name, err)
				return err
			}
			
			klog.Infof("Scaled up statefulset %s/%s to %d replicas", sts.Namespace, sts.Name, originalReplicas)
		}
	}
	
	return nil
}

// Main entry point
func main() {
	// Parse command-line flags
	var config Config
	
	// Define command-line flags
	flag.StringVar(&config.nodeSelector, "node-selector", "", "Label selector for nodes to watch (e.g. 'role=worker')")
	flag.StringVar(&config.namespace, "namespace", "", "Namespace to watch for resources (empty for all namespaces)")
	flag.StringVar(&config.kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	flag.StringVar(&config.masterURL, "master", "", "The address of the Kubernetes API server")
	
	klog.InitFlags(nil)
	flag.Parse()
	
	// Set up signals so we handle the shutdown signal gracefully
	stopCh := setupSignalHandler()
	
	// Create Kubernetes client
	var kubeClient kubernetes.Interface
	var err error
	
	if config.kubeconfig == "" {
		// Use in-cluster config
		klog.Info("Using in-cluster configuration")
		restConfig, err := rest.InClusterConfig()
		if err != nil {
			klog.Fatalf("Error building in-cluster config: %s", err.Error())
		}
		kubeClient, err = kubernetes.NewForConfig(restConfig)
	} else {
		// Use provided kubeconfig
		restConfig, err := clientcmd.BuildConfigFromFlags(config.masterURL, config.kubeconfig)
		if err != nil {
			klog.Fatalf("Error building kubeconfig: %s", err.Error())
		}
		kubeClient, err = kubernetes.NewForConfig(restConfig)
	}
	
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}
	
	// Parse node selector
	selector, err := labels.Parse(config.nodeSelector)
	if err != nil {
		klog.Fatalf("Error parsing node selector: %s", err.Error())
	}
	
	// Create informer factory
	informerFactory := informers.NewSharedInformerFactory(kubeClient, defaultResyncPeriod)
	
	// Create controller
	controller := NewController(kubeClient, informerFactory, selector, config.namespace)
	
	// Set up leader election
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName = "node-down-scaler-unknown"
	}
	
	hostname, err := os.Hostname()
	if err != nil {
		klog.Fatalf("Error getting hostname: %s", err.Error())
	}
	
	id := hostname + "_" + podName
	
	// Configure leader election
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      "node-down-scaler",
			Namespace: "node-down-scaler",
		},
		Client: kubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}
	
	leaderelection.RunOrDie(context.Background(), leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// Start informers
				informerFactory.Start(stopCh)
				
				// Start controller
				if err = controller.Run(2, stopCh); err != nil {
					klog.Fatalf("Error running controller: %s", err.Error())
				}
			},
			OnStoppedLeading: func() {
				klog.Info("Leader lost")
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				if identity == id {
					klog.Info("I am the leader")
				} else {
					klog.Infof("New leader elected: %s", identity)
				}
			},
		},
	})
}

// setupSignalHandler registers for SIGTERM and SIGINT
func setupSignalHandler() <-chan struct{} {
	stopCh := make(chan struct{})
	
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	
	go func() {
		<-c
		close(stopCh)
		<-c
		os.Exit(1) // second signal, hard exit
	}()
	
	return stopCh
}
