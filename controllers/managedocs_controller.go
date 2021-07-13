/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"

	opv1a1 "github.com/operator-framework/api/pkg/operators/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/go-logr/logr"
	ocsv1 "github.com/openshift/ocs-operator/pkg/apis/ocs/v1"
	v1 "github.com/openshift/ocs-osd-deployer/api/v1alpha1"
	"github.com/openshift/ocs-osd-deployer/templates"
	"github.com/openshift/ocs-osd-deployer/utils"
	promv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	promv1a1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1alpha1"
)

const (
	ManagedOCSFinalizer = "managedocs.ocs.openshift.io"
)

const (
	managedOCSName                         = "managedocs"
	storageClusterName                     = "ocs-storagecluster"
	prometheusName                         = "managed-ocs-prometheus"
	alertmanagerName                       = "managed-ocs-alertmanager"
	alertmanagerConfigName                 = "managed-ocs-alertmanager-config"
	dmsRuleName                            = "dms-monitor-rule"
	storageClassSizeKey                    = "size"
	enableMCGKey                           = "enable-mcg"
	deviceSetName                          = "default"
	storageClassRbdName                    = "ocs-storagecluster-ceph-rbd"
	storageClassCephFSName                 = "ocs-storagecluster-cephfs"
	deployerCSVPrefix                      = "ocs-osd-deployer"
	ocsOperatorName                        = "ocs-operator"
	monLabelKey                            = "app"
	monLabelValue                          = "managed-ocs"
	rookConfigMapName                      = "rook-ceph-operator-config"
	k8sMetricsServiceMonitorName           = "k8s-metrics-service-monitor"
	grafanaDatasourceSecretName            = "grafana-datasources"
	grafanaDatasourceSecretKey             = "prometheus.yaml"
	k8sMetricsServiceMonitorAuthSecretName = "k8s-metrics-service-monitor-auth"
	openshiftMonitoringNamespace           = "openshift-monitoring"
)

// ManagedOCSReconciler reconciles a ManagedOCS object
type ManagedOCSReconciler struct {
	Client             client.Client
	UnrestrictedClient client.Client
	Log                logr.Logger
	Scheme             *runtime.Scheme

	AddonParamSecretName         string
	AddonConfigMapName           string
	AddonConfigMapDeleteLabelKey string
	DeployerSubscriptionName     string
	PagerdutySecretName          string
	DeadMansSnitchSecretName     string
	SOPEndpoint                  string

	ctx                                context.Context
	managedOCS                         *v1.ManagedOCS
	storageCluster                     *ocsv1.StorageCluster
	prometheus                         *promv1.Prometheus
	dmsRule                            *promv1.PrometheusRule
	alertmanager                       *promv1.Alertmanager
	pagerdutySecret                    *corev1.Secret
	deadMansSnitchSecret               *corev1.Secret
	alertmanagerConfig                 *promv1a1.AlertmanagerConfig
	k8sMetricsServiceMonitor           *promv1.ServiceMonitor
	k8sMetricsServiceMonitorAuthSecret *corev1.Secret
	namespace                          string
	reconcileStrategy                  v1.ReconcileStrategy
}

// Add necessary rbac permissions for managedocs finalizer in order to set blockOwnerDeletion.
// +kubebuilder:rbac:groups=ocs.openshift.io,namespace=system,resources={managedocs,managedocs/finalizers},verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ocs.openshift.io,namespace=system,resources=managedocs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ocs.openshift.io,namespace=system,resources=storageclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ocs.openshift.io,namespace=system,resources=ocsinitializations,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="monitoring.coreos.com",namespace=system,resources={alertmanagers,prometheuses,alertmanagerconfigs},verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="monitoring.coreos.com",namespace=system,resources=prometheusrules,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="monitoring.coreos.com",namespace=system,resources=podmonitors,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="monitoring.coreos.com",namespace=system,resources=servicemonitors,verbs=get;list;watch;update;patch;create;delete
// +kubebuilder:rbac:groups="",namespace=system,resources=secrets,verbs=create;get;list;watch;update
// +kubebuilder:rbac:groups=operators.coreos.com,namespace=system,resources=subscriptions,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=operators.coreos.com,namespace=system,resources=clusterserviceversions,verbs=get;list;watch;delete;update;patch
// +kubebuilder:rbac:groups="apps",namespace=system,resources=statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources={persistentvolumeclaims,secrets},verbs=get;list;watch

// SetupWithManager creates an setup a ManagedOCSReconciler to work with the provided manager
func (r *ManagedOCSReconciler) SetupWithManager(mgr ctrl.Manager) error {
	ctrlOptions := controller.Options{
		MaxConcurrentReconciles: 1,
	}
	managedOCSPredicates := builder.WithPredicates(
		predicate.GenerationChangedPredicate{},
	)
	secretPredicates := builder.WithPredicates(
		predicate.NewPredicateFuncs(
			func(meta metav1.Object, _ runtime.Object) bool {
				name := meta.GetName()
				return name == r.AddonParamSecretName ||
					name == r.PagerdutySecretName ||
					name == r.DeadMansSnitchSecretName
			},
		),
	)
	configMapPredicates := builder.WithPredicates(
		predicate.NewPredicateFuncs(
			func(meta metav1.Object, _ runtime.Object) bool {
				name := meta.GetName()
				if name == r.AddonConfigMapName {
					if _, ok := meta.GetLabels()[r.AddonConfigMapDeleteLabelKey]; ok {
						return true
					}
				} else if name == rookConfigMapName {
					return true
				}
				return false
			},
		),
	)
	monResourcesPredicates := builder.WithPredicates(
		predicate.NewPredicateFuncs(
			func(meta metav1.Object, _ runtime.Object) bool {
				labels := meta.GetLabels()
				return labels == nil || labels[monLabelKey] != monLabelValue
			},
		),
	)
	monStatefulSetPredicates := builder.WithPredicates(
		predicate.NewPredicateFuncs(
			func(meta metav1.Object, _ runtime.Object) bool {
				name := meta.GetName()
				return name == fmt.Sprintf("prometheus-%s", prometheusName) ||
					name == fmt.Sprintf("alertmanager-%s", alertmanagerName)
			},
		),
	)
	prometheusRulesPredicates := builder.WithPredicates(
		predicate.NewPredicateFuncs(
			func(meta metav1.Object, _ runtime.Object) bool {
				labels := meta.GetLabels()
				return labels == nil || labels[monLabelKey] != monLabelValue
			},
		),
	)
	ocsCSVPredicates := builder.WithPredicates(
		predicate.NewPredicateFuncs(
			func(meta metav1.Object, _ runtime.Object) bool {
				return strings.HasPrefix(meta.GetName(), ocsOperatorName)
			},
		),
	)
	enqueueManangedOCSRequest := handler.EnqueueRequestsFromMapFunc{
		ToRequests: handler.ToRequestsFunc(
			func(obj handler.MapObject) []reconcile.Request {
				return []reconcile.Request{{
					NamespacedName: types.NamespacedName{
						Name:      managedOCSName,
						Namespace: obj.Meta.GetNamespace(),
					},
				}}
			},
		),
	}

	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(ctrlOptions).
		For(&v1.ManagedOCS{}, managedOCSPredicates).

		// Watch owned resources
		Owns(&ocsv1.StorageCluster{}).
		Owns(&promv1.Prometheus{}).
		Owns(&promv1.Alertmanager{}).
		Owns(&promv1a1.AlertmanagerConfig{}).
		Owns(&promv1.PrometheusRule{}).
		Owns(&promv1.ServiceMonitor{}).

		// Watch non-owned resources
		Watches(
			&source.Kind{Type: &corev1.Secret{}},
			&enqueueManangedOCSRequest,
			secretPredicates,
		).
		Watches(
			&source.Kind{Type: &corev1.ConfigMap{}},
			&enqueueManangedOCSRequest,
			configMapPredicates,
		).
		Watches(
			&source.Kind{Type: &promv1.PodMonitor{}},
			&enqueueManangedOCSRequest,
			monResourcesPredicates,
		).
		Watches(
			&source.Kind{Type: &promv1.ServiceMonitor{}},
			&enqueueManangedOCSRequest,
			monResourcesPredicates,
		).
		Watches(
			&source.Kind{Type: &promv1.PrometheusRule{}},
			&enqueueManangedOCSRequest,
			prometheusRulesPredicates,
		).
		Watches(
			&source.Kind{Type: &appsv1.StatefulSet{}},
			&enqueueManangedOCSRequest,
			monStatefulSetPredicates,
		).
		Watches(
			&source.Kind{Type: &ocsv1.OCSInitialization{}},
			&enqueueManangedOCSRequest,
		).
		Watches(
			&source.Kind{Type: &opv1a1.ClusterServiceVersion{}},
			&enqueueManangedOCSRequest,
			ocsCSVPredicates,
		).

		// Create the controller
		Complete(r)
}

// Reconcile changes to all owned resource based on the infromation provided by the ManagedOCS resource
func (r *ManagedOCSReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("req.Namespace", req.Namespace, "req.Name", req.Name)
	log.Info("Starting reconcile for ManagedOCS")

	// Initalize the reconciler properties from the request
	r.initReconciler(req)

	// Load the managed ocs resource (input)
	if err := r.get(r.managedOCS); err != nil {
		if errors.IsNotFound(err) {
			r.Log.V(-1).Info("ManagedOCS resource not found")
		} else {
			return ctrl.Result{}, err
		}
	}

	// Run the reconcile phases
	result, err := r.reconcilePhases()
	if err != nil {
		r.Log.Error(err, "An error was encountered during reconcilePhases")
	}

	// Ensure status is updated once even on failed reconciles
	var statusErr error
	if r.managedOCS.UID != "" {
		statusErr = r.Client.Status().Update(r.ctx, r.managedOCS)
	}

	// Reconcile errors have priority to status update errors
	if err != nil {
		return ctrl.Result{}, err
	} else if statusErr != nil {
		return ctrl.Result{}, statusErr
	} else {
		return result, nil
	}
}

func (r *ManagedOCSReconciler) initReconciler(req ctrl.Request) {
	r.ctx = context.Background()
	r.namespace = req.NamespacedName.Namespace

	r.managedOCS = &v1.ManagedOCS{}
	r.managedOCS.Name = req.NamespacedName.Name
	r.managedOCS.Namespace = r.namespace

	r.storageCluster = &ocsv1.StorageCluster{}
	r.storageCluster.Name = storageClusterName
	r.storageCluster.Namespace = r.namespace

	r.prometheus = &promv1.Prometheus{}
	r.prometheus.Name = prometheusName
	r.prometheus.Namespace = r.namespace

	r.dmsRule = &promv1.PrometheusRule{}
	r.dmsRule.Name = dmsRuleName
	r.dmsRule.Namespace = r.namespace

	r.alertmanager = &promv1.Alertmanager{}
	r.alertmanager.Name = alertmanagerName
	r.alertmanager.Namespace = r.namespace

	r.pagerdutySecret = &corev1.Secret{}
	r.pagerdutySecret.Name = r.PagerdutySecretName
	r.pagerdutySecret.Namespace = r.namespace

	r.deadMansSnitchSecret = &corev1.Secret{}
	r.deadMansSnitchSecret.Name = r.DeadMansSnitchSecretName
	r.deadMansSnitchSecret.Namespace = r.namespace

	r.alertmanagerConfig = &promv1a1.AlertmanagerConfig{}
	r.alertmanagerConfig.Name = alertmanagerConfigName
	r.alertmanagerConfig.Namespace = r.namespace

	r.k8sMetricsServiceMonitor = &promv1.ServiceMonitor{}
	r.k8sMetricsServiceMonitor.Name = k8sMetricsServiceMonitorName
	r.k8sMetricsServiceMonitor.Namespace = r.namespace

	r.k8sMetricsServiceMonitorAuthSecret = &corev1.Secret{}
	r.k8sMetricsServiceMonitorAuthSecret.Name = k8sMetricsServiceMonitorAuthSecretName
	r.k8sMetricsServiceMonitorAuthSecret.Namespace = r.namespace

}

func (r *ManagedOCSReconciler) reconcilePhases() (reconcile.Result, error) {
	// Uninstallation depends on the status of the components.
	// We are checking the uninstallation condition before getting the component status
	// to mitigate scenarios where changes to the component status occurs while the uninstallation logic is running.
	initiateUninstall := r.checkUninstallCondition()
	// Update the status of the components
	r.updateComponentStatus()

	if !r.managedOCS.DeletionTimestamp.IsZero() {
		if r.verifyComponentsDoNotExist() {
			r.Log.Info("removing finalizer from the ManagedOCS resource")
			r.managedOCS.SetFinalizers(utils.Remove(r.managedOCS.GetFinalizers(), ManagedOCSFinalizer))
			if err := r.Client.Update(r.ctx, r.managedOCS); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to remove finalizer from managedOCS: %v", err)
			}
			r.Log.Info("finallizer removed successfully")

		} else {
			// Storage cluster needs to be deleted before we delete the CSV so we can not leave it to the
			// k8s garbage collector to delete it
			r.Log.Info("deleting storagecluster")
			if err := r.delete(r.storageCluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("unable to delete storagecluster: %v", err)
			}
		}

	} else if r.managedOCS.UID != "" {
		if !utils.Contains(r.managedOCS.GetFinalizers(), ManagedOCSFinalizer) {
			r.Log.V(-1).Info("finalizer missing on the managedOCS resource, adding...")
			r.managedOCS.SetFinalizers(append(r.managedOCS.GetFinalizers(), ManagedOCSFinalizer))
			if err := r.update(r.managedOCS); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to update managedOCS with finalizer: %v", err)
			}
		}

		// Find the effective reconcile strategy
		r.reconcileStrategy = v1.ReconcileStrategyStrict
		if strings.EqualFold(string(r.managedOCS.Spec.ReconcileStrategy), string(v1.ReconcileStrategyNone)) {
			r.reconcileStrategy = v1.ReconcileStrategyNone
		}

		// Reconcile the different resources
		if err := r.reconcileRookCephOperatorConfig(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileStorageCluster(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileOCSCSV(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcilePrometheus(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileAlertmanager(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileAlertmanagerConfig(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileK8SMetricsServiceMonitorAuthSecret(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileK8SMetricsServiceMonitor(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileMonitoringResources(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileDMSPrometheusRule(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileOCSInitialization(); err != nil {
			return ctrl.Result{}, err
		}

		r.managedOCS.Status.ReconcileStrategy = r.reconcileStrategy

		// Check if we need and can uninstall
		if initiateUninstall && r.areComponentsReadyForUninstall() {
			found, err := r.findOCSVolumeClaims()
			if err != nil {
				return ctrl.Result{}, err
			}
			if found {
				r.Log.Info("Found consumer PVCs using OCS storageclasses, cannot proceed on uninstallation")
				return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
			}

			r.Log.Info("starting OCS uninstallation - deleting managedocs")
			if err := r.delete(r.managedOCS); err != nil {
				return ctrl.Result{}, fmt.Errorf("unable to delete managedocs: %v", err)
			}
		}

	} else if initiateUninstall {
		return ctrl.Result{}, r.removeOLMComponents()
	}

	return ctrl.Result{}, nil
}

func (r *ManagedOCSReconciler) updateComponentStatus() {
	// Getting the status of the StorageCluster component.
	scStatus := &r.managedOCS.Status.Components.StorageCluster
	if err := r.get(r.storageCluster); err == nil {
		if r.storageCluster.Status.Phase == "Ready" {
			scStatus.State = v1.ComponentReady
		} else {
			scStatus.State = v1.ComponentPending
		}
	} else if errors.IsNotFound(err) {
		scStatus.State = v1.ComponentNotFound
	} else {
		r.Log.V(-1).Info("error getting StorageCluster, setting compoment status to Unknown")
		scStatus.State = v1.ComponentUnknown
	}

	// Getting the status of the Prometheus component.
	promStatus := &r.managedOCS.Status.Components.Prometheus
	if err := r.get(r.prometheus); err == nil {
		promStatefulSet := &appsv1.StatefulSet{}
		promStatefulSet.Namespace = r.namespace
		promStatefulSet.Name = fmt.Sprintf("prometheus-%s", prometheusName)
		if err := r.get(promStatefulSet); err == nil {
			desiredReplicas := int32(1)
			if r.prometheus.Spec.Replicas != nil {
				desiredReplicas = *r.prometheus.Spec.Replicas
			}
			if promStatefulSet.Status.ReadyReplicas != desiredReplicas {
				promStatus.State = v1.ComponentPending
			} else {
				promStatus.State = v1.ComponentReady
			}
		} else {
			promStatus.State = v1.ComponentPending
		}
	} else if errors.IsNotFound(err) {
		promStatus.State = v1.ComponentNotFound
	} else {
		r.Log.V(-1).Info("error getting Prometheus, setting compoment status to Unknown")
		promStatus.State = v1.ComponentUnknown
	}

	// Getting the status of the Alertmanager component.
	amStatus := &r.managedOCS.Status.Components.Alertmanager
	if err := r.get(r.alertmanager); err == nil {
		amStatefulSet := &appsv1.StatefulSet{}
		amStatefulSet.Namespace = r.namespace
		amStatefulSet.Name = fmt.Sprintf("alertmanager-%s", alertmanagerName)
		if err := r.get(amStatefulSet); err == nil {
			desiredReplicas := int32(1)
			if r.alertmanager.Spec.Replicas != nil {
				desiredReplicas = *r.alertmanager.Spec.Replicas
			}
			if amStatefulSet.Status.ReadyReplicas != desiredReplicas {
				amStatus.State = v1.ComponentPending
			} else {
				amStatus.State = v1.ComponentReady
			}
		} else {
			amStatus.State = v1.ComponentPending
		}
	} else if errors.IsNotFound(err) {
		amStatus.State = v1.ComponentNotFound
	} else {
		r.Log.V(-1).Info("error getting Alertmanager, setting compoment status to Unknown")
		amStatus.State = v1.ComponentUnknown
	}
}

func (r *ManagedOCSReconciler) verifyComponentsDoNotExist() bool {
	subComponent := r.managedOCS.Status.Components

	if subComponent.StorageCluster.State == v1.ComponentNotFound {
		return true
	}
	return false
}

func (r *ManagedOCSReconciler) reconcileStorageCluster() error {
	r.Log.Info("Reconciling StorageCluster")

	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.storageCluster, func() error {
		if err := r.own(r.storageCluster); err != nil {
			return err
		}

		// Handle only strict mode reconciliation
		if r.reconcileStrategy == v1.ReconcileStrategyStrict {
			// Get an instance of the desired state
			desired := templates.StorageClusterTemplate.DeepCopy()
			if err := r.updateStorageClusterFromAddonParamsSecret(desired); err != nil {
				return err
			}

			// Override storage cluster spec with desired spec from the template.
			// We do not replace meta or status on purpose
			r.storageCluster.Spec = desired.Spec
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (r *ManagedOCSReconciler) updateStorageClusterFromAddonParamsSecret(sc *ocsv1.StorageCluster) error {
	// The addon param secret will contain the capacity of the cluster in Ti
	// size = 1,  creates a cluster of 1 Ti capacity
	// size = 2,  creates a cluster of 2 Ti capacity etc

	addonParamSecret := &corev1.Secret{}
	addonParamSecret.Name = r.AddonParamSecretName
	addonParamSecret.Namespace = r.namespace
	if err := r.get(addonParamSecret); err != nil {
		// Do not create the StorageCluster if the we fail to get the addon param secret
		return fmt.Errorf("Failed to get the addon param secret, Secret Name: %v", r.AddonParamSecretName)
	}
	addonParams := addonParamSecret.Data

	sizeAsString := string(addonParams[storageClassSizeKey])
	enableMCGAsString := string(addonParams[enableMCGKey])
	r.Log.Info("Requested add-on settings", storageClassSizeKey, sizeAsString, enableMCGKey, enableMCGAsString)
	desiredDeviceSetCount, err := strconv.Atoi(sizeAsString)
	if err != nil {
		return fmt.Errorf("Invalid storage cluster size value: %v", sizeAsString)
	}

	// Get the storage device set count of the current storage cluster
	currDeviceSetCount := 0
	for index := range r.storageCluster.Spec.StorageDeviceSets {
		item := &r.storageCluster.Spec.StorageDeviceSets[index]
		if item.Name == deviceSetName {
			currDeviceSetCount = item.Count
			break
		}
	}

	var ds *ocsv1.StorageDeviceSet = nil
	for index := range sc.Spec.StorageDeviceSets {
		item := &sc.Spec.StorageDeviceSets[index]
		if item.Name == deviceSetName {
			ds = item
			break
		}
	}
	if ds == nil {
		return fmt.Errorf("could not find default device set on stroage cluster")
	}

	// Prevent downscaling by comparing count from secret and count from storage cluster
	r.Log.Info("Setting storage device set count", "Current", currDeviceSetCount, "New", desiredDeviceSetCount)
	if currDeviceSetCount <= desiredDeviceSetCount {
		ds.Count = desiredDeviceSetCount
	} else {
		r.Log.V(-1).Info("Requested storage device set count will result in downscaling, which is not supported. Skipping")
		ds.Count = currDeviceSetCount
	}
	// Check and enable MCG in Storage Cluster spec
	mcgEnable, err := strconv.ParseBool(enableMCGAsString)
	if err != nil {
		return fmt.Errorf("Invalid Enable MCG value: %v", enableMCGAsString)
	}
	if mcgEnable {
		r.Log.Info("Enabling Multi Cloud Gateway")
		sc.Spec.MultiCloudGateway.ReconcileStrategy = "manage"
	} else if sc.Spec.MultiCloudGateway.ReconcileStrategy == "manage" {
		r.Log.V(-1).Info("Trying to disable Multi Cloud Gateway, Invalid operation")
	}

	return nil
}

func (r *ManagedOCSReconciler) reconcilePrometheus() error {
	r.Log.Info("Reconciling Prometheus")

	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.prometheus, func() error {
		if err := r.own(r.prometheus); err != nil {
			return err
		}

		desired := templates.PrometheusTemplate.DeepCopy()
		r.prometheus.ObjectMeta.Labels = map[string]string{monLabelKey: monLabelValue}
		r.prometheus.Spec = desired.Spec
		r.prometheus.Spec.Alerting.Alertmanagers[0].Namespace = r.namespace

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (r *ManagedOCSReconciler) reconcileDMSPrometheusRule() error {
	r.Log.Info("Reconciling DMS Prometheus Rule")

	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.dmsRule, func() error {
		if err := r.own(r.dmsRule); err != nil {
			return err
		}

		desired := templates.DMSPrometheusRuleTemplate.DeepCopy()
		r.dmsRule.Spec = desired.Spec

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (r *ManagedOCSReconciler) reconcileOCSInitialization() error {
	r.Log.Info("Reconciling OCSInitialization")

	ocsInitList := ocsv1.OCSInitializationList{}
	if err := r.list(&ocsInitList); err != nil {
		return fmt.Errorf("Could to list OCSInitialization resources: %v", err)
	}
	if len(ocsInitList.Items) == 0 {
		r.Log.V(-1).Info("OCSInitialization resource not found")
	} else {
		obj := &ocsInitList.Items[0]
		if !obj.Spec.EnableCephTools {
			obj.Spec.EnableCephTools = true
			if err := r.update(obj); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ManagedOCSReconciler) reconcileAlertmanager() error {
	r.Log.Info("Reconciling Alertmanager")
	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.alertmanager, func() error {
		if err := r.own(r.alertmanager); err != nil {
			return err
		}

		desired := templates.AlertmanagerTemplate.DeepCopy()
		desired.Spec.AlertmanagerConfigSelector = &metav1.LabelSelector{
			MatchLabels: map[string]string{
				monLabelKey: monLabelValue,
			},
		}
		r.alertmanager.Spec = desired.Spec
		utils.AddLabel(r.alertmanager, monLabelKey, monLabelValue)

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *ManagedOCSReconciler) reconcileAlertmanagerConfig() error {
	r.Log.Info("Reconciling AlertmanagerConfig secret")

	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.alertmanagerConfig, func() error {
		if err := r.own(r.alertmanagerConfig); err != nil {
			return err
		}

		if err := r.get(r.pagerdutySecret); err != nil {
			return fmt.Errorf("Unable to get pagerduty secret: %v", err)
		}
		pagerdutySecretData := r.pagerdutySecret.Data
		pagerdutyServiceKey := string(pagerdutySecretData["PAGERDUTY_KEY"])
		if pagerdutyServiceKey == "" {
			return fmt.Errorf("Pagerduty secret does not contain a PAGERDUTY_KEY entry")
		}

		if err := r.get(r.deadMansSnitchSecret); err != nil {
			return fmt.Errorf("Unable to get DeadMan's Snitch secret: %v", err)
		}
		dmsURL := string(r.deadMansSnitchSecret.Data["SNITCH_URL"])
		if dmsURL == "" {
			return fmt.Errorf("DeadMan's Snitch secret does not contain a SNITCH_URL entry")
		}

		desired := templates.AlertmanagerConfigTemplate.DeepCopy()
		for i := range desired.Spec.Receivers {
			receiver := &desired.Spec.Receivers[i]
			switch receiver.Name {
			case "pagerduty":
				receiver.PagerDutyConfigs[0].ServiceKey.Key = "PAGERDUTY_KEY"
				receiver.PagerDutyConfigs[0].ServiceKey.LocalObjectReference.Name = r.PagerdutySecretName
				receiver.PagerDutyConfigs[0].Details[0].Key = "SOP"
				receiver.PagerDutyConfigs[0].Details[0].Value = r.SOPEndpoint
			case "DeadMansSnitch":
				receiver.WebhookConfigs[0].URL = &dmsURL
			}
		}
		r.alertmanagerConfig.Spec = desired.Spec
		utils.AddLabel(r.alertmanagerConfig, monLabelKey, monLabelValue)

		return nil
	})

	return err
}

func (r *ManagedOCSReconciler) reconcileK8SMetricsServiceMonitorAuthSecret() error {
	r.Log.Info("Reconciling k8sMetricsServiceMonitorAuthSecret")

	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.k8sMetricsServiceMonitorAuthSecret, func() error {
		if err := r.own(r.k8sMetricsServiceMonitorAuthSecret); err != nil {
			return err
		}

		secret := &corev1.Secret{}
		secret.Name = grafanaDatasourceSecretName
		secret.Namespace = openshiftMonitoringNamespace
		if err := r.unrestrictedGet(secret); err != nil {
			return fmt.Errorf("Failed to get grafana-datasources secret from openshift-monitoring namespace: %v", err)
		}

		authInfoStructure := struct {
			DataSources []struct {
				BasicAuthPassword string `json:"basicAuthPassword"`
				BasicAuthUser     string `json:"basicAuthUser"`
			} `json:"datasources"`
		}{}

		if err := json.Unmarshal(secret.Data[grafanaDatasourceSecretKey], &authInfoStructure); err != nil {
			return fmt.Errorf("Could not unmarshal Grapana datasource data: %v", err)
		}

		r.k8sMetricsServiceMonitorAuthSecret.Data = nil
		for key := range authInfoStructure.DataSources {
			ds := &authInfoStructure.DataSources[key]
			if ds.BasicAuthUser == "internal" && ds.BasicAuthPassword != "" {
				r.k8sMetricsServiceMonitorAuthSecret.Data = map[string][]byte{
					"Username": []byte(ds.BasicAuthUser),
					"Password": []byte(ds.BasicAuthPassword),
				}
			}
		}
		if r.k8sMetricsServiceMonitorAuthSecret.Data == nil {
			return fmt.Errorf("Grapana datasource does not contain the needed credentials")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("Failed to update k8sMetricsServiceMonitorAuthSecret: %v", err)
	}
	return nil
}

func (r *ManagedOCSReconciler) reconcileK8SMetricsServiceMonitor() error {
	r.Log.Info("Reconciling k8sMetricsServiceMonitor")

	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.k8sMetricsServiceMonitor, func() error {
		if err := r.own(r.k8sMetricsServiceMonitor); err != nil {
			return err
		}
		desired := templates.K8sMetricsServiceMonitorTemplate.DeepCopy()
		r.k8sMetricsServiceMonitor.Spec = desired.Spec
		return nil
	})
	if err != nil {
		return fmt.Errorf("Failed to update k8sMetricsServiceMonitor: %v", err)
	}
	return nil
}

// reconcileMonitoringResources labels all monitoring resources (ServiceMonitors, PodMonitors, and PrometheusRules)
// found in the target namespace with a label that matches the label selector the defined on the Prometheus resource
// we are reconciling in reconcilePrometheus. Doing so instructs the Prometheus instance to notice and react to these labeled
// monitoring resources
func (r *ManagedOCSReconciler) reconcileMonitoringResources() error {
	r.Log.Info("reconciling monitoring resources")

	podMonitorList := promv1.PodMonitorList{}
	if err := r.list(&podMonitorList); err != nil {
		return fmt.Errorf("Could not list pod monitors: %v", err)
	}
	for i := range podMonitorList.Items {
		obj := podMonitorList.Items[i]
		utils.AddLabel(obj, monLabelKey, monLabelValue)
		if err := r.update(obj); err != nil {
			return err
		}
	}

	serviceMonitorList := promv1.ServiceMonitorList{}
	if err := r.list(&serviceMonitorList); err != nil {
		return fmt.Errorf("Could not list service monitors: %v", err)
	}
	for i := range serviceMonitorList.Items {
		obj := serviceMonitorList.Items[i]
		utils.AddLabel(obj, monLabelKey, monLabelValue)
		if err := r.update(obj); err != nil {
			return err
		}
	}

	promRuleList := promv1.PrometheusRuleList{}
	if err := r.list(&promRuleList); err != nil {
		return fmt.Errorf("Could not list prometheus rules: %v", err)
	}
	for i := range promRuleList.Items {
		obj := promRuleList.Items[i]
		utils.AddLabel(obj, monLabelKey, monLabelValue)
		if err := r.update(obj); err != nil {
			return err
		}
	}

	return nil
}

// reconcileRookCephOperatorConfig is used to set resource request and limits on csi containers
func (r *ManagedOCSReconciler) reconcileRookCephOperatorConfig() error {
	rookConfigMap := &corev1.ConfigMap{}
	rookConfigMap.Name = rookConfigMapName
	rookConfigMap.Namespace = r.namespace

	if err := r.get(rookConfigMap); err != nil {
		// Because resource limits will not be set, failure to get the Rook ConfigMap results in failure to reconcile.
		return fmt.Errorf("Failed to get Rook ConfigMap: %v", err)
	}

	if rookConfigMap.Data == nil {
		rookConfigMap.Data = map[string]string{}
	}

	rbdProvisionerRequirements := utils.MarshalRookResourceRequirements(utils.RookResourceRequirementsList{
		{
			Name:     "csi-provisioner",
			Resource: utils.GetResourceRequirements("csi-provisioner"),
		},
		{
			Name:     "csi-resizer",
			Resource: utils.GetResourceRequirements("csi-resizer"),
		},
		{
			Name:     "csi-attacher",
			Resource: utils.GetResourceRequirements("csi-attacher"),
		},
		{
			Name:     "csi-snapshotter",
			Resource: utils.GetResourceRequirements("csi-snapshotter"),
		},
		{
			Name:     "csi-rbdplugin",
			Resource: utils.GetResourceRequirements("csi-rbdplugin"),
		},
		{
			Name:     "liveness-prometheus",
			Resource: utils.GetResourceRequirements("liveness-prometheus"),
		},
	})

	rbdPluginRequirements := utils.MarshalRookResourceRequirements(utils.RookResourceRequirementsList{
		{
			Name:     "driver-registar",
			Resource: utils.GetResourceRequirements("driver-registrar"),
		},
		{
			Name:     "csi-rbdplugin",
			Resource: utils.GetResourceRequirements("csi-rbdplugin"),
		},
		{
			Name:     "liveness-prometheus",
			Resource: utils.GetResourceRequirements("liveness-prometheus"),
		},
	})

	fsProvisionerRequirements := utils.MarshalRookResourceRequirements(utils.RookResourceRequirementsList{
		{
			Name:     "csi-provisioner",
			Resource: utils.GetResourceRequirements("csi-provisioner"),
		},
		{
			Name:     "csi-resizer",
			Resource: utils.GetResourceRequirements("csi-resizer"),
		},
		{
			Name:     "csi-attacher",
			Resource: utils.GetResourceRequirements("csi-attacher"),
		},
		{
			Name:     "csi-cephfsplugin",
			Resource: utils.GetResourceRequirements("csi-cephfsplugin"),
		},
		{
			Name:     "liveness-prometheus",
			Resource: utils.GetResourceRequirements("liveness-prometheus"),
		},
	})

	fsPluginRequirements := utils.MarshalRookResourceRequirements(utils.RookResourceRequirementsList{
		{
			Name:     "driver-registrar",
			Resource: utils.GetResourceRequirements("driver-registrar"),
		},
		{
			Name:     "csi-cephfsplugin",
			Resource: utils.GetResourceRequirements("csi-cephfsplugin"),
		},
		{
			Name:     "liveness-prometheus",
			Resource: utils.GetResourceRequirements("liveness-prometheus"),
		},
	})

	if rookConfigMap.Data["CSI_RBD_PROVISIONER_RESOURCE"] != rbdProvisionerRequirements ||
		rookConfigMap.Data["CSI_RBD_PLUGIN_RESOURCE"] != rbdPluginRequirements ||
		rookConfigMap.Data["CSI_CEPHFS_PROVISIONER_RESOURCE"] != fsProvisionerRequirements ||
		rookConfigMap.Data["CSI_CEPHFS_PLUGIN_RESOURCE"] != fsPluginRequirements {

		rookConfigMap.Data["CSI_RBD_PROVISIONER_RESOURCE"] = rbdProvisionerRequirements
		rookConfigMap.Data["CSI_RBD_PLUGIN_RESOURCE"] = rbdPluginRequirements
		rookConfigMap.Data["CSI_CEPHFS_PROVISIONER_RESOURCE"] = fsProvisionerRequirements
		rookConfigMap.Data["CSI_CEPHFS_PLUGIN_RESOURCE"] = fsPluginRequirements

		if err := r.update(rookConfigMap); err != nil {
			return fmt.Errorf("Failed to update Rook ConfigMap: %v", err)
		}

	}

	return nil
}

func (r *ManagedOCSReconciler) checkUninstallCondition() bool {
	configmap := &corev1.ConfigMap{}
	configmap.Name = r.AddonConfigMapName
	configmap.Namespace = r.namespace

	err := r.get(configmap)
	if err != nil {
		if !errors.IsNotFound(err) {
			r.Log.Error(err, "Unable to get addon delete configmap")
		}
		return false
	}
	_, ok := configmap.Labels[r.AddonConfigMapDeleteLabelKey]
	return ok
}

func (r *ManagedOCSReconciler) areComponentsReadyForUninstall() bool {
	subComponents := r.managedOCS.Status.Components
	return subComponents.StorageCluster.State == v1.ComponentReady &&
		subComponents.Prometheus.State == v1.ComponentReady &&
		subComponents.Alertmanager.State == v1.ComponentReady
}

func (r *ManagedOCSReconciler) findOCSVolumeClaims() (bool, error) {
	pvcList := &corev1.PersistentVolumeClaimList{}
	err := r.UnrestrictedClient.List(r.ctx, pvcList)
	if err != nil {
		return false, fmt.Errorf("unable to list pvcs: %v", err)
	}
	for i := range pvcList.Items {
		scName := *pvcList.Items[i].Spec.StorageClassName
		if scName == storageClassCephFSName || scName == storageClassRbdName {
			return true, nil
		}
	}
	return false, nil
}

func (r *ManagedOCSReconciler) reconcileOCSCSV() error {
	csvList := opv1a1.ClusterServiceVersionList{}
	if err := r.list(&csvList); err != nil {
		return fmt.Errorf("unable to list csv resources: %v", err)
	}

	csv := getCSVByPrefix(csvList, ocsOperatorName)
	if csv == nil {
		return fmt.Errorf("OCS CSV not found")
	}
	var isChanged bool
	deployments := csv.Spec.InstallStrategy.StrategySpec.DeploymentSpecs
	for i := range deployments {
		containers := deployments[i].Spec.Template.Spec.Containers
		for j := range containers {
			switch container := &containers[j]; container.Name {
			case "ocs-operator":
				resources := utils.GetResourceRequirements("ocs-operator")
				if !equality.Semantic.DeepEqual(container.Resources, resources) {
					container.Resources = resources
					isChanged = true
				}
			case "rook-ceph-operator":
				resources := utils.GetResourceRequirements("rook-ceph-operator")
				if !equality.Semantic.DeepEqual(container.Resources, resources) {
					container.Resources = resources
					isChanged = true
				}
			case "ocs-metrics-exporter":
				resources := utils.GetResourceRequirements("ocs-metrics-exporter")
				if !equality.Semantic.DeepEqual(container.Resources, resources) {
					container.Resources = resources
					isChanged = true
				}
			default:
				r.Log.V(-1).Info("Could not find resource requirement", "Resource", container.Name)
			}
		}
	}
	if isChanged {
		if err := r.update(csv); err != nil {
			return fmt.Errorf("Failed to update OCS CSV with resource requirements: %v", err)
		}
	}
	return nil
}

func (r *ManagedOCSReconciler) removeOLMComponents() error {

	r.Log.Info("Deleting subscription")
	subscription := &opv1a1.Subscription{}
	subscription.Namespace = r.namespace
	subscription.Name = r.DeployerSubscriptionName
	if err := r.delete(subscription); err != nil {
		return fmt.Errorf("unable to delete the deployer subscription: %v", err)
	}
	r.Log.Info("deployer subscription removed successfully")

	r.Log.Info("deleting deployer csv")
	csvList := opv1a1.ClusterServiceVersionList{}
	if err := r.list(&csvList); err != nil {
		return fmt.Errorf("unable to list csv resources: %v", err)
	}

	csv := getCSVByPrefix(csvList, deployerCSVPrefix)
	if csv != nil {
		if err := r.delete(csv); err != nil {
			return fmt.Errorf("Unable to delete csv: %v", err)
		}
	}

	r.Log.Info("Deployer csv removed successfully")
	return nil
}

func (r *ManagedOCSReconciler) get(obj runtime.Object) error {
	key, err := client.ObjectKeyFromObject(obj)
	if err != nil {
		return err
	}
	return r.Client.Get(r.ctx, key, obj)
}

func (r *ManagedOCSReconciler) list(obj runtime.Object) error {
	listOptions := client.InNamespace(r.namespace)
	return r.Client.List(r.ctx, obj, listOptions)
}

func (r *ManagedOCSReconciler) update(obj runtime.Object) error {
	return r.Client.Update(r.ctx, obj)
}

func (r *ManagedOCSReconciler) delete(obj runtime.Object) error {
	if err := r.Client.Delete(r.ctx, obj); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *ManagedOCSReconciler) own(resource metav1.Object) error {
	// Ensure managedOCS ownership on a resource
	if err := ctrl.SetControllerReference(r.managedOCS, resource, r.Scheme); err != nil {
		return err
	}
	return nil
}

func getCSVByPrefix(csvList opv1a1.ClusterServiceVersionList, name string) *opv1a1.ClusterServiceVersion {
	var csv *opv1a1.ClusterServiceVersion = nil
	for index := range csvList.Items {
		candidate := &csvList.Items[index]
		if strings.HasPrefix(candidate.Name, name) {
			csv = candidate
			break
		}
	}
	return csv
}

func (r *ManagedOCSReconciler) unrestrictedGet(obj runtime.Object) error {
	key, err := client.ObjectKeyFromObject(obj)
	if err != nil {
		return err
	}
	return r.UnrestrictedClient.Get(r.ctx, key, obj)
}
