package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

const (
	// Annotation to identify resources to scale down when node is down
	scaleAnnotation = "node-down-scaler/enabled"
	// Annotation to store original replicas count
	originalReplicasAnnotation = "node-down-scaler/original-replicas"
	// Maximum retry attempts
	maxRetries = 5
)

type OperatorConfig struct {
	nodeSelector          string
	reconcileInterval     time.Duration
	leaderElectionID      string
	operatorNamespace     string
	kubeconfig            string
	leaseDuration         time.Duration
	renewDeadline         time.Duration
	retryPeriod           time.Duration
	leaderElectionEnabled bool
}

func main() {
	klog.InitFlags(nil)
	config := parseFlags()

	// Create kubernetes client
	kubeClient, err := createKubeClient(config.kubeconfig)
	if err != nil {
		klog.Fatalf("Error creating kubernetes client: %v", err)
	}

	// Setup leader election if needed
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle termination signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		klog.Info("Received termination signal, shutting down...")
		cancel()
		os.Exit(0)
	}()

	if config.leaderElectionEnabled {
		runWithLeaderElection(ctx, kubeClient, config, runOperator)
	} else {
		runOperator(ctx, kubeClient, config)
	}
}

func parseFlags() *OperatorConfig {
	config := &OperatorConfig{}

	flag.StringVar(&config.nodeSelector, "node-selector", "role=worker", "Label selector to filter nodes")
	flag.DurationVar(&config.reconcileInterval, "reconcile-interval", 30*time.Second, "Reconciliation interval")
	flag.StringVar(&config.leaderElectionID, "leader-election-id", "node-down-scaler-lock", "Leader election resource name")
	flag.StringVar(&config.operatorNamespace, "namespace", "node-down-scaler", "Namespace where operator is deployed")
	flag.StringVar(&config.kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	flag.DurationVar(&config.leaseDuration, "lease-duration", 15*time.Second, "Leader election lease duration")
	flag.DurationVar(&config.renewDeadline, "renew-deadline", 10*time.Second, "Leader election renew deadline")
	flag.DurationVar(&config.retryPeriod, "retry-period", 2*time.Second, "Leader election retry period")
	flag.BoolVar(&config.leaderElectionEnabled, "enable-leader-election", true, "Enable leader election")

	flag.Parse()

	return config
}

func createKubeClient(kubeconfigPath string) (*kubernetes.Clientset, error) {
	var config *rest.Config
	var err error

	if kubeconfigPath == "" {
		// Use in-cluster config if no kubeconfig provided
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("error creating in-cluster config: %v", err)
		}
	} else {
		// Use kubeconfig file
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("error creating config from kubeconfig: %v", err)
		}
	}

	return kubernetes.NewForConfig(config)
}

func runWithLeaderElection(ctx context.Context, kubeClient *kubernetes.Clientset, config *OperatorConfig, callback func(context.Context, *kubernetes.Clientset, *OperatorConfig)) {
	// Get hostname to identify this instance
	hostname, err := os.Hostname()
	if err != nil {
		klog.Fatalf("Error getting hostname: %v", err)
	}

	id := hostname + "_" + string(time.Now().UnixNano())

	// Setup leader election config
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      config.leaderElectionID,
			Namespace: config.operatorNamespace,
		},
		Client: kubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   config.leaseDuration,
		RenewDeadline:   config.renewDeadline,
		RetryPeriod:     config.retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				klog.Info("Started leading, running operator...")
				callback(ctx, kubeClient, config)
			},
			OnStoppedLeading: func() {
				klog.Info("Leader election lost")
			},
			OnNewLeader: func(identity string) {
				if identity != id {
					klog.Infof("New leader elected: %s", identity)
				}
			},
		},
	})
}

func runOperator(ctx context.Context, kubeClient *kubernetes.Clientset, config *OperatorConfig) {
	klog.Infof("Starting Node Down Scaler operator with node selector: %s", config.nodeSelector)

	// Setup reconciliation loop
	ticker := time.NewTicker(config.reconcileInterval)
	defer ticker.Stop()

	// Initial reconciliation
	reconcile(ctx, kubeClient, config.nodeSelector)

	// Reconciliation loop
	for {
		select {
		case <-ticker.C:
			reconcile(ctx, kubeClient, config.nodeSelector)
		case <-ctx.Done():
			klog.Info("Shutting down operator")
			return
		}
	}
}

func reconcile(ctx context.Context, kubeClient *kubernetes.Clientset, nodeSelector string) {
	klog.V(3).Info("Starting reconciliation")

	// Get nodes matching selector
	selector, err := labels.Parse(nodeSelector)
	if err != nil {
		klog.Errorf("Error parsing node selector: %v", err)
		return
	}

	nodes, err := kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})

	if err != nil {
		klog.Errorf("Error listing nodes: %v", err)
		return
	}

	// Check if any node is down
	nodeDown := false
	for _, node := range nodes.Items {
		if isNodeDown(&node) {
			klog.Infof("Node %s is down", node.Name)
			nodeDown = true
			break
		}
	}

	// Process all deployments
	deployments, err := kubeClient.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Error listing deployments: %v", err)
		return
	}

	for _, deployment := range deployments.Items {
		if val, exists := deployment.Annotations[scaleAnnotation]; exists && val == "true" {
			if nodeDown {
				scaleResourceDown(ctx, kubeClient, "deployment", deployment.Namespace, deployment.Name)
			} else {
				scaleResourceUp(ctx, kubeClient, "deployment", deployment.Namespace, deployment.Name)
			}
		}
	}

	// Process all statefulsets
	statefulsets, err := kubeClient.AppsV1().StatefulSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Error listing statefulsets: %v", err)
		return
	}

	for _, statefulset := range statefulsets.Items {
		if val, exists := statefulset.Annotations[scaleAnnotation]; exists && val == "true" {
			if nodeDown {
				scaleResourceDown(ctx, kubeClient, "statefulset", statefulset.Namespace, statefulset.Name)
			} else {
				scaleResourceUp(ctx, kubeClient, "statefulset", statefulset.Namespace, statefulset.Name)
			}
		}
	}

	klog.V(3).Info("Reconciliation completed")
}

// Check if a node is considered down
func isNodeDown(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status != corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// Scale down a deployment or statefulset to 0 replicas
func scaleResourceDown(ctx context.Context, kubeClient *kubernetes.Clientset, resourceType, namespace, name string) {
	klog.Infof("Scaling down %s %s/%s", resourceType, namespace, name)

	switch resourceType {
	case "deployment":
		// Use RetryOnConflict to automatically retry in case of conflicts
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// Get the latest version of the deployment
			deployment, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					klog.Warningf("Deployment %s/%s not found", namespace, name)
					return nil
				}
				return err
			}

			// Check if already scaled down
			if deployment.Spec.Replicas != nil && *deployment.Spec.Replicas == 0 {
				klog.V(3).Infof("Deployment %s/%s already scaled to 0", namespace, name)
				return nil
			}

			// Store original replicas if not already annotated
			if _, exists := deployment.Annotations[originalReplicasAnnotation]; !exists {
				// Ensure annotations map exists
				if deployment.Annotations == nil {
					deployment.Annotations = make(map[string]string)
				}

				// Store original replicas
				if deployment.Spec.Replicas != nil {
					deployment.Annotations[originalReplicasAnnotation] = fmt.Sprintf("%d", *deployment.Spec.Replicas)
				} else {
					deployment.Annotations[originalReplicasAnnotation] = "1" // Default value
				}

				// Update annotations first
				_, err = kubeClient.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
				if err != nil {
					return err
				}

				// Get the latest version again after annotation update
				deployment, err = kubeClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
				if err != nil {
					return err
				}
			}

			// Scale to 0
			zero := int32(0)
			deployment.Spec.Replicas = &zero
			_, err = kubeClient.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
			return err
		})

		if err != nil {
			klog.Errorf("Error scaling down deployment %s/%s after retries: %v", namespace, name, err)
		} else {
			klog.Infof("Successfully scaled down deployment %s/%s to 0", namespace, name)
		}

	case "statefulset":
		// Use RetryOnConflict to automatically retry in case of conflicts
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// Get the latest version of the statefulset
			statefulset, err := kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					klog.Warningf("StatefulSet %s/%s not found", namespace, name)
					return nil
				}
				return err
			}

			// Check if already scaled down
			if statefulset.Spec.Replicas != nil && *statefulset.Spec.Replicas == 0 {
				klog.V(3).Infof("StatefulSet %s/%s already scaled to 0", namespace, name)
				return nil
			}

			// Store original replicas if not already annotated
			if _, exists := statefulset.Annotations[originalReplicasAnnotation]; !exists {
				// Ensure annotations map exists
				if statefulset.Annotations == nil {
					statefulset.Annotations = make(map[string]string)
				}

				// Store original replicas
				if statefulset.Spec.Replicas != nil {
					statefulset.Annotations[originalReplicasAnnotation] = fmt.Sprintf("%d", *statefulset.Spec.Replicas)
				} else {
					statefulset.Annotations[originalReplicasAnnotation] = "1" // Default value
				}

				// Update annotations first
				_, err = kubeClient.AppsV1().StatefulSets(namespace).Update(ctx, statefulset, metav1.UpdateOptions{})
				if err != nil {
					return err
				}

				// Get the latest version again after annotation update
				statefulset, err = kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
				if err != nil {
					return err
				}
			}

			// Scale to 0
			zero := int32(0)
			statefulset.Spec.Replicas = &zero
			_, err = kubeClient.AppsV1().StatefulSets(namespace).Update(ctx, statefulset, metav1.UpdateOptions{})
			return err
		})

		if err != nil {
			klog.Errorf("Error scaling down statefulset %s/%s after retries: %v", namespace, name, err)
		} else {
			klog.Infof("Successfully scaled down statefulset %s/%s to 0", namespace, name)
		}
	}
}

// Scale up a deployment or statefulset to its original replicas count
func scaleResourceUp(ctx context.Context, kubeClient *kubernetes.Clientset, resourceType, namespace, name string) {
	klog.Infof("Scaling up %s %s/%s", resourceType, namespace, name)

	switch resourceType {
	case "deployment":
		// Use RetryOnConflict to automatically retry in case of conflicts
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// Get the latest version of the deployment
			deployment, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					klog.Warningf("Deployment %s/%s not found", namespace, name)
					return nil
				}
				return err
			}

			// Get original replicas from annotation
			originalReplicasStr, exists := deployment.Annotations[originalReplicasAnnotation]
			if !exists {
				klog.V(3).Infof("Deployment %s/%s has no original replicas annotation", namespace, name)
				return nil
			}

			// Parse original replicas
			originalReplicasInt, err := strconv.Atoi(originalReplicasStr)
			if err != nil {
				return fmt.Errorf("error parsing original replicas annotation: %v", err)
			}
			originalReplicas := int32(originalReplicasInt)

			// Check if already scaled up
			if deployment.Spec.Replicas != nil && *deployment.Spec.Replicas == originalReplicas {
				klog.V(3).Infof("Deployment %s/%s already scaled to original replicas: %d", namespace, name, originalReplicas)
				return nil
			}

			// Scale back to original
			deployment.Spec.Replicas = &originalReplicas
			_, err = kubeClient.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
			if err != nil {
				return err
			}

			// Get the latest version again after scale update
			deployment, err = kubeClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}

			// Remove annotation
			delete(deployment.Annotations, originalReplicasAnnotation)
			_, err = kubeClient.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
			return err
		})

		if err != nil {
			klog.Errorf("Error scaling up deployment %s/%s after retries: %v", namespace, name, err)
		} else {
			klog.Infof("Successfully scaled up deployment %s/%s to original replica count", namespace, name)
		}

	case "statefulset"::po
		// Use RetryOnConflict to automatically retry in case of conflicts
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// Get the latest version of the statefulset
			statefulset, err := kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					klog.Warningf("StatefulSet %s/%s not found", namespace, name)
					return nil
				}
				return err
			}

			// Get original replicas from annotation
			originalReplicasStr, exists := statefulset.Annotations[originalReplicasAnnotation]
			if !exists {
				klog.V(3).Infof("StatefulSet %s/%s has no original replicas annotation", namespace, name)
				return nil
			}

			// Parse original replicas
			originalReplicasInt, err := strconv.Atoi(originalReplicasStr)
			if err != nil {
				return fmt.Errorf("error parsing original replicas annotation: %v", err)
			}
			originalReplicas := int32(originalReplicasInt)

			// Check if already scaled up
			if statefulset.Spec.Replicas != nil && *statefulset.Spec.Replicas == originalReplicas {
				klog.V(3).Infof("StatefulSet %s/%s already scaled to original replicas: %d", namespace, name, originalReplicas)
				return nil
			}

			// Scale back to original
			statefulset.Spec.Replicas = &originalReplicas
			_, err = kubeClient.AppsV1().StatefulSets(namespace).Update(ctx, statefulset, metav1.UpdateOptions{})
			if err != nil {
				return err
			}

			// Get the latest version again after scale update
			statefulset, err = kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}

			// Remove annotation
			delete(statefulset.Annotations, originalReplicasAnnotation)
			_, err = kubeClient.AppsV1().StatefulSets(namespace).Update(ctx, statefulset, metav1.UpdateOptions{})
			return err
		})

		if err != nil {
			klog.Errorf("Error scaling up statefulset %s/%s after retries: %v", namespace, name, err)
		} else {
			klog.Infof("Successfully scaled up statefulset %s/%s to original replica count", namespace, name)
		}
	}
}
