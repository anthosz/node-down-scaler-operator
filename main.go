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
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	// Annotation to mark resources to be managed by this operator
	watchAnnotation = "node-watcher.k8s.io/watch"
	// Annotation to remember original scale
	originalScaleAnnotation = "node-watcher.k8s.io/original-scale"
)

type Controller struct {
	kubeClient     kubernetes.Interface
	nodeInformer   cache.SharedIndexInformer
	deployInformer cache.SharedIndexInformer
	stsInformer    cache.SharedIndexInformer
	workqueue      workqueue.RateLimitingInterface
	nodeLister     cache.GenericLister
	deployLister   cache.GenericLister
	stsLister      cache.GenericLister
}

func NewController(kubeClient kubernetes.Interface, factory informers.SharedInformerFactory) *Controller {
	// Set up informers
	nodeInformer := factory.Core().V1().Nodes().Informer()
	deployInformer := factory.Apps().V1().Deployments().Informer()
	stsInformer := factory.Apps().V1().StatefulSets().Informer()

	// Create controller
	controller := &Controller{
		kubeClient:     kubeClient,
		nodeInformer:   nodeInformer,
		deployInformer: deployInformer,
		stsInformer:    stsInformer,
		workqueue:      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Nodes"),
		nodeLister:     factory.Core().V1().Nodes().Lister(),
		deployLister:   factory.Apps().V1().Deployments().Lister(),
		stsLister:      factory.Apps().V1().StatefulSets().Lister(),
	}

	// Set up event handlers
	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueNode,
		UpdateFunc: func(old, new interface{}) {
			oldNode := old.(*corev1.Node)
			newNode := new.(*corev1.Node)
			if nodeReadyStatusChanged(oldNode, newNode) {
				controller.enqueueNode(newNode)
			}
		},
		DeleteFunc: controller.enqueueNode,
	})

	return controller
}

// nodeReadyStatusChanged checks if a node's ready status has changed
func nodeReadyStatusChanged(oldNode, newNode *corev1.Node) bool {
	oldReady := isNodeReady(oldNode)
	newReady := isNodeReady(newNode)
	return oldReady != newReady
}

// isNodeReady checks if a node is in Ready status
func isNodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (c *Controller) enqueueNode(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	klog.Info("Starting Node Watcher controller")

	klog.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(stopCh, 
		c.nodeInformer.HasSynced, 
		c.deployInformer.HasSynced, 
		c.stsInformer.HasSynced) {
		klog.Fatal("Failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")
}

func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if err := c.syncHandler(key); err != nil {
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.workqueue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

func (c *Controller) syncHandler(key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get node by name
	node, err := c.nodeLister.Get(name)
	if errors.IsNotFound(err) {
		klog.Infof("Node %s no longer exists", name)
		// Node is deleted, treat it as not ready
		return c.handleNodeNotReady(name)
	}
	if err != nil {
		return err
	}

	// Check if node is ready
	nodeObj := node.(*corev1.Node)
	if isNodeReady(nodeObj) {
		return c.handleNodeReady(name)
	} else {
		return c.handleNodeNotReady(name)
	}
}

func (c *Controller) handleNodeReady(nodeName string) error {
	klog.Infof("Node %s is Ready, scaling up resources if needed", nodeName)

	// Scale up deployments
	deployments, err := c.kubeClient.AppsV1().Deployments("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, deploy := range deployments.Items {
		if value, exists := deploy.Annotations[watchAnnotation]; exists && value == "true" {
			originalScaleStr, exists := deploy.Annotations[originalScaleAnnotation]
			if exists {
				c.scaleDeploymentUp(&deploy, originalScaleStr)
			}
		}
	}

	// Scale up statefulsets
	statefulsets, err := c.kubeClient.AppsV1().StatefulSets("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, sts := range statefulsets.Items {
		if value, exists := sts.Annotations[watchAnnotation]; exists && value == "true" {
			originalScaleStr, exists := sts.Annotations[originalScaleAnnotation]
			if exists {
				c.scaleStatefulSetUp(&sts, originalScaleStr)
			}
		}
	}

	return nil
}

func (c *Controller) handleNodeNotReady(nodeName string) error {
	klog.Infof("Node %s is NotReady, scaling down resources if needed", nodeName)

	// Scale down deployments
	deployments, err := c.kubeClient.AppsV1().Deployments("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, deploy := range deployments.Items {
		if value, exists := deploy.Annotations[watchAnnotation]; exists && value == "true" {
			c.scaleDeploymentDown(&deploy)
		}
	}

	// Scale down statefulsets
	statefulsets, err := c.kubeClient.AppsV1().StatefulSets("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, sts := range statefulsets.Items {
		if value, exists := sts.Annotations[watchAnnotation]; exists && value == "true" {
			c.scaleStatefulSetDown(&sts)
		}
	}

	return nil
}

func (c *Controller) scaleDeploymentDown(deploy *appsv1.Deployment) {
	// Skip if already scaled to 0
	if *deploy.Spec.Replicas == 0 {
		return
	}

	// Remember original scale
	originalScale := fmt.Sprintf("%d", *deploy.Spec.Replicas)
	
	// Update annotations
	if deploy.Annotations == nil {
		deploy.Annotations = make(map[string]string)
	}
	deploy.Annotations[originalScaleAnnotation] = originalScale
	
	// Set replicas to 0
	replicas := int32(0)
	deploy.Spec.Replicas = &replicas
	
	// Update deployment
	_, err := c.kubeClient.AppsV1().Deployments(deploy.Namespace).Update(context.Background(), deploy, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to scale down deployment %s/%s: %v", deploy.Namespace, deploy.Name, err)
		return
	}
	
	klog.Infof("Scaled down deployment %s/%s from %s to 0", deploy.Namespace, deploy.Name, originalScale)
}

func (c *Controller) scaleDeploymentUp(deploy *appsv1.Deployment, originalScaleStr string) {
	var originalScale int32
	fmt.Sscanf(originalScaleStr, "%d", &originalScale)
	
	// Skip if already scaled back
	if *deploy.Spec.Replicas > 0 {
		return
	}
	
	// Set replicas to original scale
	deploy.Spec.Replicas = &originalScale
	
	// Remove annotation
	delete(deploy.Annotations, originalScaleAnnotation)
	
	// Update deployment
	_, err := c.kubeClient.AppsV1().Deployments(deploy.Namespace).Update(context.Background(), deploy, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to scale up deployment %s/%s: %v", deploy.Namespace, deploy.Name, err)
		return
	}
	
	klog.Infof("Scaled up deployment %s/%s to %d", deploy.Namespace, deploy.Name, originalScale)
}

func (c *Controller) scaleStatefulSetDown(sts *appsv1.StatefulSet) {
	// Skip if already scaled to 0
	if *sts.Spec.Replicas == 0 {
		return
	}

	// Remember original scale
	originalScale := fmt.Sprintf("%d", *sts.Spec.Replicas)
	
	// Update annotations
	if sts.Annotations == nil {
		sts.Annotations = make(map[string]string)
	}
	sts.Annotations[originalScaleAnnotation] = originalScale
	
	// Set replicas to 0
	replicas := int32(0)
	sts.Spec.Replicas = &replicas
	
	// Update statefulset
	_, err := c.kubeClient.AppsV1().StatefulSets(sts.Namespace).Update(context.Background(), sts, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to scale down statefulset %s/%s: %v", sts.Namespace, sts.Name, err)
		return
	}
	
	klog.Infof("Scaled down statefulset %s/%s from %s to 0", sts.Namespace, sts.Name, originalScale)
}

func (c *Controller) scaleStatefulSetUp(sts *appsv1.StatefulSet, originalScaleStr string) {
	var originalScale int32
	fmt.Sscanf(originalScaleStr, "%d", &originalScale)
	
	// Skip if already scaled back
	if *sts.Spec.Replicas > 0 {
		return
	}
	
	// Set replicas to original scale
	sts.Spec.Replicas = &originalScale
	
	// Remove annotation
	delete(sts.Annotations, originalScaleAnnotation)
	
	// Update statefulset
	_, err := c.kubeClient.AppsV1().StatefulSets(sts.Namespace).Update(context.Background(), sts, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to scale up statefulset %s/%s: %v", sts.Namespace, sts.Name, err)
		return
	}
	
	klog.Infof("Scaled up statefulset %s/%s to %d", sts.Namespace, sts.Name, originalScale)
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	// Get kubeconfig
	kubeconfigPath := os.Getenv("KUBECONFIG")
	if kubeconfigPath == "" {
		kubeconfigPath = os.Getenv("HOME") + "/.kube/config"
	}

	// Create kubernetes client
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		klog.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	// Create shared informer factory
	factory := informers.NewSharedInformerFactory(kubeClient, time.Hour*12)

	// Create controller
	controller := NewController(kubeClient, factory)

	// Leader election setup
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			klog.Fatalf("Failed to get hostname: %v", err)
		}
		podName = hostname
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	// Leader election config
	leaderElectionConfig := leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Name:      "node-watcher-operator",
				Namespace: namespace,
			},
			Client: kubeClient.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: podName,
			},
		},
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// Start informers
				stopCh := make(chan struct{})
				factory.Start(stopCh)

				// Run controller
				if err := controller.Run(2, stopCh); err != nil {
					klog.Fatalf("Error running controller: %s", err.Error())
				}
			},
			OnStoppedLeading: func() {
				klog.Info("Leader election lost")
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				if identity == podName {
					klog.Info("Still the leader!")
				} else {
					klog.Infof("New leader elected: %s", identity)
				}
			},
		},
		ReleaseOnCancel: true,
	}

	// Start leader election
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalCh
		klog.Info("Received termination signal")
		cancel()
	}()

	// Start leader election
	klog.Info("Starting leader election")
	leaderelection.RunOrDie(ctx, leaderElectionConfig)
}
