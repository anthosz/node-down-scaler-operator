package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// NodeTracker keeps track of node states and original replicas
type NodeTracker struct {
	// Map to track original replica count for scaled-down resources
	originalReplicas map[string]int32
	// Map to track node states (true = healthy, false = unhealthy)
	nodeStates map[string]bool
}

// NewNodeTracker creates a new NodeTracker
func NewNodeTracker() *NodeTracker {
	return &NodeTracker{
		originalReplicas: make(map[string]int32),
		nodeStates:       make(map[string]bool),
	}
}

// Controller is the main operator controller
type Controller struct {
	clientset kubernetes.Interface
	queue     workqueue.RateLimitingInterface
	tracker   *NodeTracker
}

// NewController creates a new Controller
func NewController(clientset kubernetes.Interface) *Controller {
	return &Controller{
		clientset: clientset,
		queue:     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "node-scaler"),
		tracker:   NewNodeTracker(),
	}
}

// Run starts the controller
func (c *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Info("Starting Node Scaler Controller")

	// Initialize node states
	c.initializeNodeStates()

	// Start the workers
	go wait.Until(c.runWorker, time.Second, ctx.Done())

	// Start the node watcher
	go c.watchNodes(ctx)

	<-ctx.Done()
	klog.Info("Shutting down Node Scaler Controller")
}

// initializeNodeStates initializes the state of all nodes
func (c *Controller) initializeNodeStates() {
	nodes, err := c.clientset.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to list nodes: %v", err)
		return
	}

	for _, node := range nodes.Items {
		isReady := isNodeReady(&node)
		c.tracker.nodeStates[node.Name] = isReady
		klog.Infof("Initialized node %s with state: %v", node.Name, isReady)
	}
}

// watchNodes watches for node status changes
func (c *Controller) watchNodes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
			nodes, err := c.clientset.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
			if err != nil {
				klog.Errorf("Failed to list nodes: %v", err)
				continue
			}

			for _, node := range nodes.Items {
				currentState := isNodeReady(&node)
				previousState, exists := c.tracker.nodeStates[node.Name]

				if !exists || previousState != currentState {
					klog.Infof("Node %s state changed: %v -> %v", node.Name, previousState, currentState)
					c.tracker.nodeStates[node.Name] = currentState
					c.queue.Add(node.Name)
				}
			}
		}
	}
}

// isNodeReady checks if a node is in Ready condition
func isNodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// runWorker is a long-running function that processes work queue items
func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

// processNextItem processes the next item from the work queue
func (c *Controller) processNextItem() bool {
	obj, shutdown := c.queue.Get()
	if shutdown {
		return false
	}

	defer c.queue.Done(obj)

	err := c.handleNodeStateChange(obj.(string))
	if err != nil {
		klog.Errorf("Error handling node state change: %v", err)
		c.queue.AddRateLimited(obj)
		return true
	}

	c.queue.Forget(obj)
	return true
}

// handleNodeStateChange handles a node state change event
func (c *Controller) handleNodeStateChange(nodeName string) error {
	isReady, exists := c.tracker.nodeStates[nodeName]
	if !exists {
		return fmt.Errorf("node %s not found in tracker", nodeName)
	}

	if isReady {
		// Node is ready, restore resources
		klog.Infof("Node %s is ready, restoring resources", nodeName)
		return c.restoreResources(nodeName)
	} else {
		// Node is not ready, scale resources to 0
		klog.Infof("Node %s is not ready, scaling down resources", nodeName)
		return c.scaleDownResources(nodeName)
	}
}

// getResourceKey returns a unique key for a resource
func getResourceKey(kind, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", kind, namespace, name)
}

// scaleDownResources scales down resources on the unhealthy node
func (c *Controller) scaleDownResources(nodeName string) error {
	// Get pods running on the node
	pods, err := c.clientset.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	})
	if err != nil {
		return fmt.Errorf("failed to list pods on node %s: %v", nodeName, err)
	}

	for _, pod := range pods.Items {
		// Skip if not owned by a Deployment or StatefulSet
		if len(pod.OwnerReferences) == 0 {
			continue
		}

		ownerRef := pod.OwnerReferences[0]
		if ownerRef.Kind != "ReplicaSet" && ownerRef.Kind != "StatefulSet" {
			continue
		}

		// For ReplicaSet, find the parent Deployment
		if ownerRef.Kind == "ReplicaSet" {
			rs, err := c.clientset.AppsV1().ReplicaSets(pod.Namespace).Get(context.Background(), ownerRef.Name, metav1.GetOptions{})
			if err != nil {
				klog.Warningf("Failed to get ReplicaSet %s/%s: %v", pod.Namespace, ownerRef.Name, err)
				continue
			}

			if len(rs.OwnerReferences) == 0 {
				continue
			}

			deployOwnerRef := rs.OwnerReferences[0]
			if deployOwnerRef.Kind != "Deployment" {
				continue
			}

			// Scale down the Deployment
			deploy, err := c.clientset.AppsV1().Deployments(pod.Namespace).Get(context.Background(), deployOwnerRef.Name, metav1.GetOptions{})
			if err != nil {
				klog.Warningf("Failed to get Deployment %s/%s: %v", pod.Namespace, deployOwnerRef.Name, err)
				continue
			}

			resourceKey := getResourceKey("Deployment", deploy.Namespace, deploy.Name)
			if _, exists := c.tracker.originalReplicas[resourceKey]; !exists {
				c.tracker.originalReplicas[resourceKey] = *deploy.Spec.Replicas
				klog.Infof("Scaling down Deployment %s/%s from %d to 0", deploy.Namespace, deploy.Name, *deploy.Spec.Replicas)
				
				// Scale down to 0
				zero := int32(0)
				deploy.Spec.Replicas = &zero
				_, err = c.clientset.AppsV1().Deployments(deploy.Namespace).Update(context.Background(), deploy, metav1.UpdateOptions{})
				if err != nil {
					klog.Errorf("Failed to scale down Deployment %s/%s: %v", deploy.Namespace, deploy.Name, err)
				}
			}
		} else if ownerRef.Kind == "StatefulSet" {
			// Scale down the StatefulSet
			sts, err := c.clientset.AppsV1().StatefulSets(pod.Namespace).Get(context.Background(), ownerRef.Name, metav1.GetOptions{})
			if err != nil {
				klog.Warningf("Failed to get StatefulSet %s/%s: %v", pod.Namespace, ownerRef.Name, err)
				continue
			}

			resourceKey := getResourceKey("StatefulSet", sts.Namespace, sts.Name)
			if _, exists := c.tracker.originalReplicas[resourceKey]; !exists {
				c.tracker.originalReplicas[resourceKey] = *sts.Spec.Replicas
				klog.Infof("Scaling down StatefulSet %s/%s from %d to 0", sts.Namespace, sts.Name, *sts.Spec.Replicas)
				
				// Scale down to 0
				zero := int32(0)
				sts.Spec.Replicas = &zero
				_, err = c.clientset.AppsV1().StatefulSets(sts.Namespace).Update(context.Background(), sts, metav1.UpdateOptions{})
				if err != nil {
					klog.Errorf("Failed to scale down StatefulSet %s/%s: %v", sts.Namespace, sts.Name, err)
				}
			}
		}
	}

	return nil
}

// restoreResources restores resources that were scaled down
func (c *Controller) restoreResources(nodeName string) error {
	// Restore Deployments
	for resourceKey, replicas := range c.tracker.originalReplicas {
		kind, namespace, name, err := parseResourceKey(resourceKey)
		if err != nil {
			klog.Errorf("Failed to parse resource key %s: %v", resourceKey, err)
			continue
		}

		if kind == "Deployment" {
			deploy, err := c.clientset.AppsV1().Deployments(namespace).Get(context.Background(), name, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					delete(c.tracker.originalReplicas, resourceKey)
				}
				klog.Warningf("Failed to get Deployment %s/%s: %v", namespace, name, err)
				continue
			}

			klog.Infof("Restoring Deployment %s/%s to %d replicas", namespace, name, replicas)
			deploy.Spec.Replicas = &replicas
			_, err = c.clientset.AppsV1().Deployments(namespace).Update(context.Background(), deploy, metav1.UpdateOptions{})
			if err != nil {
				klog.Errorf("Failed to restore Deployment %s/%s: %v", namespace, name, err)
				continue
			}
			
			delete(c.tracker.originalReplicas, resourceKey)
		} else if kind == "StatefulSet" {
			sts, err := c.clientset.AppsV1().StatefulSets(namespace).Get(context.Background(), name, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					delete(c.tracker.originalReplicas, resourceKey)
				}
				klog.Warningf("Failed to get StatefulSet %s/%s: %v", namespace, name, err)
				continue
			}

			klog.Infof("Restoring StatefulSet %s/%s to %d replicas", namespace, name, replicas)
			sts.Spec.Replicas = &replicas
			_, err = c.clientset.AppsV1().StatefulSets(namespace).Update(context.Background(), sts, metav1.UpdateOptions{})
			if err != nil {
				klog.Errorf("Failed to restore StatefulSet %s/%s: %v", namespace, name, err)
				continue
			}
			
			delete(c.tracker.originalReplicas, resourceKey)
		}
	}

	return nil
}

// parseResourceKey parses a resource key into kind, namespace, and name
func parseResourceKey(key string) (string, string, string, error) {
	var kind, namespace, name string
	_, err := fmt.Sscanf(key, "%s/%s/%s", &kind, &namespace, &name)
	if err != nil {
		return "", "", "", err
	}
	return kind, namespace, name, nil
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	// Get in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		// Try to use kubeconfig if in-cluster config fails
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = os.Getenv("HOME") + "/.kube/config"
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			klog.Fatalf("Failed to get Kubernetes config: %v", err)
		}
	}

	// Create Kubernetes clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	// Create controller
	controller := NewController(clientset)

	// Set up leader election
	id := uuid.New().String()
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      "node-scaler-lock",
			Namespace: namespace,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	// Run with leader election
	leaderelection.RunOrDie(context.Background(), leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				klog.Infof("Started leading with identity %s", id)
				controller.Run(ctx)
			},
			OnStoppedLeading: func() {
				klog.Infof("Stopped leading")
			},
			OnNewLeader: func(identity string) {
				if identity != id {
					klog.Infof("New leader elected: %s", identity)
				}
			},
		},
	})
}
