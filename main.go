package main

import (
        "context"
        "flag"
        "fmt"
        "os"
        "strings"
        "time"

        corev1 "k8s.io/api/core/v1"
        appsv1 "k8s.io/api/apps/v1"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        "k8s.io/apimachinery/pkg/labels"
        "k8s.io/client-go/kubernetes"
        "k8s.io/client-go/rest"
        "k8s.io/client-go/tools/clientcmd"
        "k8s.io/client-go/tools/leaderelection"
        "k8s.io/client-go/tools/leaderelection/resourcelock"
        "k8s.io/klog/v2"
)

var (
        masterURL      string
        kubeconfig     string
        namespace      string
        labelSelector  string
        interval       int
        lockName       string
        configMapName  string
        configMapNs    string
)

type WorkloadConfig struct {
        Name      string `json:"name"`
        Namespace string `json:"namespace"`
        Type      string `json:"type"` // "deployment" ou "statefulset"
}

func init() {
        flag.StringVar(&kubeconfig, "kubeconfig", "", "Chemin vers le fichier kubeconfig")
        flag.StringVar(&masterURL, "master", "", "URL de l'API Kubernetes")
        flag.StringVar(&namespace, "namespace", "", "Namespace à surveiller (vide pour tous)")
        flag.StringVar(&labelSelector, "labels", "scale-to-zero=true", "Sélecteur de labels pour les workloads à scaler")
        flag.IntVar(&interval, "interval", 30, "Intervalle de vérification en secondes")
        flag.StringVar(&lockName, "lock-name", "node-down-scaler", "Nom de la ressource pour l'élection du leader")
        flag.StringVar(&configMapName, "config", "scale-to-zero-config", "Nom de la ConfigMap contenant la configuration des workloads")
        flag.StringVar(&configMapNs, "config-namespace", "kube-system", "Namespace de la ConfigMap de configuration")
}

func main() {
        flag.Parse()
        klog.InitFlags(nil)

        // Configuration pour se connecter au cluster
        var config *rest.Config
        var err error

        if kubeconfig == "" {
                klog.Info("Utilisation de la configuration in-cluster")
                config, err = rest.InClusterConfig()
        } else {
                klog.Infof("Utilisation du kubeconfig: %s", kubeconfig)
                config, err = clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
        }
        if err != nil {
                klog.Fatalf("Erreur lors de la création de la configuration: %s", err.Error())
        }

        // Création du client Kubernetes
        clientset, err := kubernetes.NewForConfig(config)
        if err != nil {
                klog.Fatalf("Erreur lors de la création du client: %s", err.Error())
        }

        // Configuration de l'élection du leader
        id, err := os.Hostname()
        if err != nil {
                klog.Fatalf("Erreur lors de la récupération du hostname: %s", err.Error())
        }

        // Détermination du namespace pour le lock
        lockNamespace := os.Getenv("POD_NAMESPACE")
        if lockNamespace == "" {
                lockNamespace = "default"
        }

        lock := &resourcelock.LeaseLock{
                LeaseMeta: metav1.ObjectMeta{
                        Name:      lockName,
                        Namespace: lockNamespace,
                },
                Client: clientset.CoordinationV1(),
                LockConfig: resourcelock.ResourceLockConfig{
                        Identity: id,
                },
        }

        // Démarrage de l'élection du leader
        leaderelection.RunOrDie(context.Background(), leaderelection.LeaderElectionConfig{
                Lock:            lock,
                ReleaseOnCancel: true,
                LeaseDuration:   15 * time.Second,
                RenewDeadline:   10 * time.Second,
                RetryPeriod:     2 * time.Second,
                Callbacks: leaderelection.LeaderCallbacks{
                        OnStartedLeading: func(ctx context.Context) {
                                klog.Info("Élu comme leader, démarrage des opérations")
                                run(ctx, clientset)
                        },
                        OnStoppedLeading: func() {
                                klog.Info("Leadership perdu, arrêt des opérations")
                                os.Exit(0)
                        },
                        OnNewLeader: func(identity string) {
                                if identity == id {
                                        return
                                }
                                klog.Infof("Nouveau leader élu: %s", identity)
                        },
                },
        })
}

func run(ctx context.Context, clientset *kubernetes.Clientset) {
        ticker := time.NewTicker(time.Duration(interval) * time.Second)
        defer ticker.Stop()

        for {
                select {
                case <-ctx.Done():
                        return
                case <-ticker.C:
                        handleNodeDownScaling(ctx, clientset)
                }
        }
}

func handleNodeDownScaling(ctx context.Context, clientset *kubernetes.Clientset) {
        // Récupération des nœuds en état NotReady
        nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
        if err != nil {
                klog.Errorf("Erreur lors de la récupération des nœuds: %s", err.Error())
                return
        }

        downNodes := make(map[string]bool)
        for _, node := range nodes.Items {
                for _, condition := range node.Status.Conditions {
                        if condition.Type == corev1.NodeReady && condition.Status != corev1.ConditionTrue {
                                klog.Infof("Nœud en panne détecté: %s", node.Name)
                                downNodes[node.Name] = true
                        }
                }
        }

        if len(downNodes) == 0 {
                klog.Info("Aucun nœud en panne détecté")
                return
        }

        // Récupération de la ConfigMap avec la configuration des workloads
        configMap, err := clientset.CoreV1().ConfigMaps(configMapNs).Get(ctx, configMapName, metav1.GetOptions{})
        if err != nil {
                klog.Errorf("Erreur lors de la récupération de la ConfigMap %s/%s: %s", configMapNs, configMapName, err.Error())

                // Si la ConfigMap n'existe pas, on utilise la méthode par labels
                klog.Info("Utilisation du mode de sélection par labels comme fallback")
                useLabelsBasedSelection(ctx, clientset, downNodes)
                return
        }

        // Parcourir les données de la ConfigMap pour trouver les workloads à scaler
        for key, value := range configMap.Data {
                parts := strings.Split(value, "/")
                if len(parts) != 2 {
                        klog.Warningf("Format invalide pour l'entrée %s: %s (format attendu: type/namespace)", key, value)
                        continue
                }

                workloadType := parts[0]
                workloadNamespace := parts[1]

                switch workloadType {
                case "deployment":
                        scaleSpecificDeployment(ctx, clientset, key, workloadNamespace)
                case "statefulset":
                        scaleSpecificStatefulSet(ctx, clientset, key, workloadNamespace)
                default:
                        klog.Warningf("Type de workload non supporté: %s", workloadType)
                }
        }
}

func useLabelsBasedSelection(ctx context.Context, clientset *kubernetes.Clientset, downNodes map[string]bool) {
        // Construction du sélecteur de labels
        labelReq, err := labels.ParseToRequirements(labelSelector)
        if err != nil {
                klog.Errorf("Erreur lors du parsing du sélecteur de labels: %s", err.Error())
                return
        }

        selector := labels.NewSelector()
        for _, req := range labelReq {
                selector = selector.Add(req)
        }

        // Traitement des Deployments
        scaleDeploymentsByLabels(ctx, clientset, selector)

        // Traitement des StatefulSets
        scaleStatefulSetsByLabels(ctx, clientset, selector)
}

func scaleDeploymentsByLabels(ctx context.Context, clientset *kubernetes.Clientset, selector labels.Selector) {
        var deployments *appsv1.DeploymentList
        var err error

        if namespace == "" {
                deployments, err = clientset.AppsV1().Deployments("").List(ctx, metav1.ListOptions{
                        LabelSelector: selector.String(),
                })
        } else {
                deployments, err = clientset.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{
                        LabelSelector: selector.String(),
                })
        }

        if err != nil {
                klog.Errorf("Erreur lors de la récupération des deployments: %s", err.Error())
                return
        }

        for _, deploy := range deployments.Items {
                if deploy.Spec.Replicas != nil && *deploy.Spec.Replicas > 0 {
                        scaleToZero(ctx, clientset, deploy.Name, deploy.Namespace, "deployment")
                }
        }
}

func scaleStatefulSetsByLabels(ctx context.Context, clientset *kubernetes.Clientset, selector labels.Selector) {
        var statefulSets *appsv1.StatefulSetList
        var err error

        if namespace == "" {
                statefulSets, err = clientset.AppsV1().StatefulSets("").List(ctx, metav1.ListOptions{
                        LabelSelector: selector.String(),
                })
        } else {
                statefulSets, err = clientset.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{
                        LabelSelector: selector.String(),
                })
        }

        if err != nil {
                klog.Errorf("Erreur lors de la récupération des statefulsets: %s", err.Error())
                return
        }

        for _, sts := range statefulSets.Items {
                if sts.Spec.Replicas != nil && *sts.Spec.Replicas > 0 {
                        scaleToZero(ctx, clientset, sts.Name, sts.Namespace, "statefulset")
                }
        }
}

func scaleSpecificDeployment(ctx context.Context, clientset *kubernetes.Clientset, name, workloadNamespace string) {
        deployment, err := clientset.AppsV1().Deployments(workloadNamespace).Get(ctx, name, metav1.GetOptions{})
        if err != nil {
                klog.Errorf("Erreur lors de la récupération du deployment %s/%s: %s", workloadNamespace, name, err.Error())
                return
        }

        if deployment.Spec.Replicas != nil && *deployment.Spec.Replicas > 0 {
                scaleToZero(ctx, clientset, name, workloadNamespace, "deployment")
        }
}

func scaleSpecificStatefulSet(ctx context.Context, clientset *kubernetes.Clientset, name, workloadNamespace string) {
        statefulSet, err := clientset.AppsV1().StatefulSets(workloadNamespace).Get(ctx, name, metav1.GetOptions{})
        if err != nil {
                klog.Errorf("Erreur lors de la récupération du statefulset %s/%s: %s", workloadNamespace, name, err.Error())
                return
        }

        if statefulSet.Spec.Replicas != nil && *statefulSet.Spec.Replicas > 0 {
                scaleToZero(ctx, clientset, name, workloadNamespace, "statefulset")
        }
}

func scaleToZero(ctx context.Context, clientset *kubernetes.Clientset, name, ns, kind string) {
        klog.Infof("Scaling à zéro le %s %s/%s", kind, ns, name)

        if kind == "deployment" {
                deployment, err := clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
                if err != nil {
                        klog.Errorf("Erreur lors de la récupération du deployment %s/%s: %s", ns, name, err.Error())
                        return
                }

                // Sauvegarder le nombre de replicas actuel comme annotation
                if deployment.Annotations == nil {
                        deployment.Annotations = make(map[string]string)
                }

                // Ne pas écraser l'annotation si elle existe déjà
                if _, exists := deployment.Annotations["original-replicas"]; !exists {
                        deployment.Annotations["original-replicas"] = fmt.Sprintf("%d", *deployment.Spec.Replicas)
                }

                // Mise à jour des replicas à 0
                zero := int32(0)
                deployment.Spec.Replicas = &zero

                _, err = clientset.AppsV1().Deployments(ns).Update(ctx, deployment, metav1.UpdateOptions{})
                if err != nil {
                        klog.Errorf("Erreur lors du scaling du deployment %s/%s: %s", ns, name, err.Error())
                } else {
                        klog.Infof("Deployment %s/%s scaled à zéro avec succès", ns, name)
                }
        } else if kind == "statefulset" {
                statefulSet, err := clientset.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
                if err != nil {
                        klog.Errorf("Erreur lors de la récupération du statefulset %s/%s: %s", ns, name, err.Error())
                        return
                }

                // Sauvegarder le nombre de replicas actuel comme annotation
                if statefulSet.Annotations == nil {
                        statefulSet.Annotations = make(map[string]string)
                }

                // Ne pas écraser l'annotation si elle existe déjà
                if _, exists := statefulSet.Annotations["original-replicas"]; !exists {
                        statefulSet.Annotations["original-replicas"] = fmt.Sprintf("%d", *statefulSet.Spec.Replicas)
                }

                // Mise à jour des replicas à 0
                zero := int32(0)
                statefulSet.Spec.Replicas = &zero

                _, err = clientset.AppsV1().StatefulSets(ns).Update(ctx, statefulSet, metav1.UpdateOptions{})
                if err != nil {
                        klog.Errorf("Erreur lors du scaling du statefulset %s/%s: %s", ns, name, err.Error())
                } else {
                        klog.Infof("StatefulSet %s/%s scaled à zéro avec succès", ns, name)
                }
        }
}
