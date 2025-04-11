package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

const (
	// Annotation to enable scaling to zero when nodes are down
	autoScaleAnnotation = "node-down-scaler/enabled"
	// Annotation to store original replicas count
	originalReplicasAnnotation = "node-down-scaler/original-replicas"
)

var (
	masterURL  string
	kubeconfig string
	nodeLabel  string
	namespace  string
	leaseName  string
	checkInterval int
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	// Set up the client config
	cfg, err := buildConfig(masterURL, kubeconfig)
	if err != nil {
		klog.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	// Create the clientset
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	// Create the dynamic client
	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building kubernetes dynamic client: %s", err.Error())
	}

	// Get hostname for leader election ID
	hostname, err := os.Hostname()
	if err != nil {
		klog.Fatalf("Error getting hostname: %s", err.Error())
	}

	id := hostname + "_" + fmt.Sprintf("%d", time.Now().Unix())

	// Leader election configuration
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
		},
		Client: kubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	// Start leader election
	leaderelection.RunOrDie(context.Background(), leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// Start the controller when we become the leader
				controller := NewController(kubeClient, dynamicClient, nodeLabel, time.Duration(checkInterval) * time.Second)
				controller.Run(ctx)
			},
			OnStoppedLeading: func() {
				klog.Info("Leader election lost")
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				if identity == id {
					klog.Info("Still the leader!")
					return
				}
				klog.Infof("New leader elected: %s", identity)
			},
		},
	})
}

// Controller manages the scaling of resources based on node status
type Controller struct {
	kubeClient    kubernetes.Interface
	dynamicClient dynamic.Interface
	nodeLabel     string
	checkInterval time.Duration
}

// NewController creates a new controller
func NewController(kubeClient kubernetes.Interface, dynamicClient dynamic.Interface, nodeLabel string, checkInterval time.Duration) *Controller {
	return &Controller{
		kubeClient:    kubeClient,
		dynamicClient: dynamicClient,
		nodeLabel:     nodeLabel,
		checkInterval: checkInterval,
	}
}

// Run starts the controller
func (c *Controller) Run(ctx context.Context) {
	defer runtime.HandleCrash()

	klog.Info("Starting node-down-scaler controller")

	// Run the reconciliation loop
	go wait.Until(func() {
		if err := c.reconcile(); err != nil {
			klog.Errorf("Error during reconciliation: %v", err)
		}
	}, c.checkInterval, ctx.Done())

	<-ctx.Done()
	klog.Info("Shutting down node-down-scaler controller")
}

// reconcile checks the node status and scales resources accordingly
func (c *Controller) reconcile() error {
	nodesAreDown, err := c.areNodesDown()
	if err != nil {
		return fmt.Errorf("failed to check nodes status: %v", err)
	}

	if nodesAreDown {
		klog.Info("Detected nodes with the specified label are down, scaling resources to 0")
		if err := c.scaleResourcesToZero(); err != nil {
			return fmt.Errorf("failed to scale resources to zero: %v", err)
		}
	} else {
		klog.Info("All nodes with the specified label are up, restoring original scales")
		if err := c.restoreOriginalScale(); err != nil {
			return fmt.Errorf("failed to restore original scale: %v", err)
		}
	}

	return nil
}

// areNodesDown checks if any nodes with the specified label are not ready
func (c *Controller) areNodesDown() (bool, error) {
	nodes, err := c.kubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{
		LabelSelector: c.nodeLabel,
	})
	if err != nil {
		return false, err
	}

	if len(nodes.Items) == 0 {
		klog.Warning("No nodes found with label selector: %s", c.nodeLabel)
		return false, nil
	}

	for _, node := range nodes.Items {
		for _, condition := range node.Status.Conditions {
			if condition.Type == "Ready" && condition.Status != "True" {
				klog.Infof("Node %s is not ready", node.Name)
				return true, nil
			}
		}
	}

	return false, nil
}

// scaleResourcesToZero scales down deployments and statefulsets with the auto-scale annotation
func (c *Controller) scaleResourcesToZero() error {
	// Handle Deployments
	if err := c.scaleDeploymentsToZero(); err != nil {
		return err
	}

	// Handle StatefulSets
	if err := c.scaleStatefulSetsToZero(); err != nil {
		return err
	}

	return nil
}

// scaleDeploymentsToZero scales all deployments with the annotation to zero
func (c *Controller) scaleDeploymentsToZero() error {
	deploymentRes := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	
	// List all deployments in all namespaces
	deployments, err := c.dynamicClient.Resource(deploymentRes).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, deployment := range deployments.Items {
		annotations := deployment.GetAnnotations()
		if annotations == nil {
			continue
		}

		_, hasAnnotation := annotations[autoScaleAnnotation]
		if !hasAnnotation {
			continue
		}

		namespace := deployment.GetNamespace()
		name := deployment.GetName()

		// Get the current number of replicas
		replicas, found, err := unstructured.NestedInt64(deployment.Object, "spec", "replicas")
		if err != nil || !found {
			klog.Errorf("Failed to get replicas for deployment %s/%s: %v", namespace, name, err)
			continue
		}

		// If already scaled to zero, skip
		if replicas == 0 {
			continue
		}

		klog.Infof("Scaling deployment %s/%s to 0", namespace, name)

		// Store original replicas in annotation if not already stored
		if _, hasOriginal := annotations[originalReplicasAnnotation]; !hasOriginal {
			annotations[originalReplicasAnnotation] = fmt.Sprintf("%d", replicas)
			deployment.SetAnnotations(annotations)
		}

		// Update deployment with retries on conflict
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// Get the latest version before update
			currentDeployment, err := c.dynamicClient.Resource(deploymentRes).Namespace(namespace).Get(context.TODO(), name, metav1.GetOptions{})
			if err != nil {
				return err
			}

			// Set replicas to 0
			err = unstructured.SetNestedField(currentDeployment.Object, int64(0), "spec", "replicas")
			if err != nil {
				return err
			}

			// Also ensure the annotation is preserved
			currentAnnotations := currentDeployment.GetAnnotations()
			if currentAnnotations == nil {
				currentAnnotations = make(map[string]string)
			}
			currentAnnotations[originalReplicasAnnotation] = annotations[originalReplicasAnnotation]
			currentDeployment.SetAnnotations(currentAnnotations)

			// Update the deployment
			_, err = c.dynamicClient.Resource(deploymentRes).Namespace(namespace).Update(context.TODO(), currentDeployment, metav1.UpdateOptions{})
			return err
		})

		if err != nil {
			klog.Errorf("Failed to scale deployment %s/%s to zero: %v", namespace, name, err)
		}
	}

	return nil
}

// scaleStatefulSetsToZero scales all statefulsets with the annotation to zero
func (c *Controller) scaleStatefulSetsToZero() error {
	stsRes := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}
	
	// List all statefulsets in all namespaces
	statefulsets, err := c.dynamicClient.Resource(stsRes).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, sts := range statefulsets.Items {
		annotations := sts.GetAnnotations()
		if annotations == nil {
			continue
		}

		_, hasAnnotation := annotations[autoScaleAnnotation]
		if !hasAnnotation {
			continue
		}

		namespace := sts.GetNamespace()
		name := sts.GetName()

		// Get the current number of replicas
		replicas, found, err := unstructured.NestedInt64(sts.Object, "spec", "replicas")
		if err != nil || !found {
			klog.Errorf("Failed to get replicas for statefulset %s/%s: %v", namespace, name, err)
			continue
		}

		// If already scaled to zero, skip
		if replicas == 0 {
			continue
		}

		klog.Infof("Scaling statefulset %s/%s to 0", namespace, name)

		// Store original replicas in annotation if not already stored
		if _, hasOriginal := annotations[originalReplicasAnnotation]; !hasOriginal {
			annotations[originalReplicasAnnotation] = fmt.Sprintf("%d", replicas)
			sts.SetAnnotations(annotations)
		}

		// Update statefulset with retries on conflict
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// Get the latest version before update
			currentSts, err := c.dynamicClient.Resource(stsRes).Namespace(namespace).Get(context.TODO(), name, metav1.GetOptions{})
			if err != nil {
				return err
			}

			// Set replicas to 0
			err = unstructured.SetNestedField(currentSts.Object, int64(0), "spec", "replicas")
			if err != nil {
				return err
			}

			// Also ensure the annotation is preserved
			currentAnnotations := currentSts.GetAnnotations()
			if currentAnnotations == nil {
				currentAnnotations = make(map[string]string)
			}
			currentAnnotations[originalReplicasAnnotation] = annotations[originalReplicasAnnotation]
			currentSts.SetAnnotations(currentAnnotations)

			// Update the statefulset
			_, err = c.dynamicClient.Resource(stsRes).Namespace(namespace).Update(context.TODO(), currentSts, metav1.UpdateOptions{})
			return err
		})

		if err != nil {
			klog.Errorf("Failed to scale statefulset %s/%s to zero: %v", namespace, name, err)
		}
	}

	return nil
}

// restoreOriginalScale restores the original scale of resources if all nodes are up
func (c *Controller) restoreOriginalScale() error {
	// Restore Deployments
	if err := c.restoreDeployments(); err != nil {
		return err
	}

	// Restore StatefulSets
	if err := c.restoreStatefulSets(); err != nil {
		return err
	}

	return nil
}

// restoreDeployments restores the original scale for deployments
func (c *Controller) restoreDeployments() error {
	deploymentRes := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	
	// List all deployments in all namespaces
	deployments, err := c.dynamicClient.Resource(deploymentRes).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, deployment := range deployments.Items {
		annotations := deployment.GetAnnotations()
		if annotations == nil {
			continue
		}

		_, hasAnnotation := annotations[autoScaleAnnotation]
		originalReplicas, hasOriginal := annotations[originalReplicasAnnotation]
		if !hasAnnotation || !hasOriginal {
			continue
		}

		namespace := deployment.GetNamespace()
		name := deployment.GetName()

		// Get the current number of replicas
		currentReplicas, found, err := unstructured.NestedInt64(deployment.Object, "spec", "replicas")
		if err != nil || !found {
			klog.Errorf("Failed to get replicas for deployment %s/%s: %v", namespace, name, err)
			continue
		}

		// Convert originalReplicas from string to int64
		var originalReplicasInt int64
		_, err = fmt.Sscanf(originalReplicas, "%d", &originalReplicasInt)
		if err != nil {
			klog.Errorf("Failed to parse original replicas %s for deployment %s/%s: %v", originalReplicas, namespace, name, err)
			continue
		}

		// If already restored, skip
		if currentReplicas == originalReplicasInt {
			continue
		}

		klog.Infof("Restoring deployment %s/%s to %d replicas", namespace, name, originalReplicasInt)

		// Update deployment with retries on conflict
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// Get the latest version before update
			currentDeployment, err := c.dynamicClient.Resource(deploymentRes).Namespace(namespace).Get(context.TODO(), name, metav1.GetOptions{})
			if err != nil {
				return err
			}

			// Set replicas to original value
			err = unstructured.SetNestedField(currentDeployment.Object, originalReplicasInt, "spec", "replicas")
			if err != nil {
				return err
			}

			// Update the deployment
			_, err = c.dynamicClient.Resource(deploymentRes).Namespace(namespace).Update(context.TODO(), currentDeployment, metav1.UpdateOptions{})
			return err
		})

		if err != nil {
			klog.Errorf("Failed to restore deployment %s/%s to original scale: %v", namespace, name, err)
		}
	}

	return nil
}

// restoreStatefulSets restores the original scale for statefulsets
func (c *Controller) restoreStatefulSets() error {
	stsRes := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}
	
	// List all statefulsets in all namespaces
	statefulsets, err := c.dynamicClient.Resource(stsRes).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, sts := range statefulsets.Items {
		annotations := sts.GetAnnotations()
		if annotations == nil {
			continue
		}

		_, hasAnnotation := annotations[autoScaleAnnotation]
		originalReplicas, hasOriginal := annotations[originalReplicasAnnotation]
		if !hasAnnotation || !hasOriginal {
			continue
		}

		namespace := sts.GetNamespace()
		name := sts.GetName()

		// Get the current number of replicas
		currentReplicas, found, err := unstructured.NestedInt64(sts.Object, "spec", "replicas")
		if err != nil || !found {
			klog.Errorf("Failed to get replicas for statefulset %s/%s: %v", namespace, name, err)
			continue
		}

		// Convert originalReplicas from string to int64
		var originalReplicasInt int64
		_, err = fmt.Sscanf(originalReplicas, "%d", &originalReplicasInt)
		if err != nil {
			klog.Errorf("Failed to parse original replicas %s for statefulset %s/%s: %v", originalReplicas, namespace, name, err)
			continue
		}

		// If already restored, skip
		if currentReplicas == originalReplicasInt {
			continue
		}

		klog.Infof("Restoring statefulset %s/%s to %d replicas", namespace, name, originalReplicasInt)

		// Update statefulset with retries on conflict
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// Get the latest version before update
			currentSts, err := c.dynamicClient.Resource(stsRes).Namespace(namespace).Get(context.TODO(), name, metav1.GetOptions{})
			if err != nil {
				return err
			}

			// Set replicas to original value
			err = unstructured.SetNestedField(currentSts.Object, originalReplicasInt, "spec", "replicas")
			if err != nil {
				return err
			}

			// Update the statefulset
			_, err = c.dynamicClient.Resource(stsRes).Namespace(namespace).Update(context.TODO(), currentSts, metav1.UpdateOptions{})
			return err
		})

		if err != nil {
			klog.Errorf("Failed to restore statefulset %s/%s to original scale: %v", namespace, name, err)
		}
	}

	return nil
}

// buildConfig builds the client config
func buildConfig(masterURL, kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	}
	return rest.InClusterConfig()
}

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&nodeLabel, "node-label", "", "Label selector to identify the nodes to monitor")
	flag.StringVar(&namespace, "namespace", "node-down-scaler", "Namespace for the operator")
	flag.StringVar(&leaseName, "lease-name", "node-down-scaler-leader", "Name of the lease object for leader election")
	flag.IntVar(&checkInterval, "check-interval", 30, "Interval in seconds between node status checks")
}
