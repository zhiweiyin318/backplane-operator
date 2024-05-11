// Copyright Contributors to the Open Cluster Management project

/*
Copyright 2021.

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

//go:generate go run pkg/templates/rbac.go

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/stolostron/backplane-operator/controllers/webhookcert"
	"github.com/stolostron/backplane-operator/pkg/servingcert"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"time"

	operatorsapiv2 "github.com/operator-framework/api/pkg/operators/v2"
	backplanev1 "github.com/stolostron/backplane-operator/api/v1"
	"github.com/stolostron/backplane-operator/controllers"
	renderer "github.com/stolostron/backplane-operator/pkg/rendering"
	"github.com/stolostron/backplane-operator/pkg/status"
	"github.com/stolostron/backplane-operator/pkg/utils"
	"github.com/stolostron/backplane-operator/pkg/version"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clustermanager "open-cluster-management.io/api/operator/v1"

	configv1 "github.com/openshift/api/config/v1"
	operatorv1 "github.com/openshift/api/operator/v1"
	hiveconfig "github.com/openshift/hive/apis/hive/v1"
	rbacv1 "k8s.io/api/rbac/v1"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/util/retry"

	"go.uber.org/zap/zapcore"
	admissionregistration "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apixv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	// +kubebuilder:scaffold:imports
)

const (
	crdName = "multiclusterengines.multicluster.openshift.io"
	crdsDir = "pkg/templates/crds"
)

var (
	cacheDuration time.Duration = time.Minute * 5
	scheme                      = runtime.NewScheme()
	setupLog                    = ctrl.Log.WithName("setup")
)

func init() {
	if _, exists := os.LookupEnv("OPERATOR_VERSION"); !exists {
		panic("OPERATOR_VERSION not defined")
	}

	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(backplanev1.AddToScheme(scheme))

	utilruntime.Must(apiregistrationv1.AddToScheme(scheme))

	utilruntime.Must(operatorsapiv2.AddToScheme(scheme))

	utilruntime.Must(admissionregistration.AddToScheme(scheme))

	utilruntime.Must(apixv1.AddToScheme(scheme))

	utilruntime.Must(hiveconfig.AddToScheme(scheme))

	utilruntime.Must(clustermanager.AddToScheme(scheme))

	utilruntime.Must(monitoringv1.AddToScheme(scheme))

	utilruntime.Must(configv1.AddToScheme(scheme))

	utilruntime.Must(operatorv1.AddToScheme(scheme))

	// +kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string

	var leaseDuration time.Duration
	var renewDeadline time.Duration
	var retryPeriod time.Duration
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")

	flag.DurationVar(&leaseDuration, "leader-election-lease-duration", 137*time.Second, ""+
		"The duration that non-leader candidates will wait after observing a leadership "+
		"renewal until attempting to acquire leadership of a led but unrenewed leader "+
		"slot. This is effectively the maximum duration that a leader can be stopped "+
		"before it is replaced by another candidate. This is only applicable if leader "+
		"election is enabled.")
	flag.DurationVar(&renewDeadline, "leader-election-renew-deadline", 107*time.Second, ""+
		"The interval between attempts by the acting master to renew a leadership slot "+
		"before it stops leading. This must be less than or equal to the lease duration. "+
		"This is only applicable if leader election is enabled.")
	flag.DurationVar(&retryPeriod, "leader-election-retry-period", 26*time.Second, ""+
		"The duration the clients should wait between attempting acquisition and renewal "+
		"of a leadership. This is only applicable if leader election is enabled.")
	opts := zap.Options{
		Development: true,
		TimeEncoder: zapcore.ISO8601TimeEncoder,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	err := utils.DetectOpenShift(ctrl.GetConfigOrDie())
	if err != nil {
		setupLog.Error(err, "unable to detect if cluster is openShift")
		os.Exit(1)
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	ctrl.Log.WithName("Backplane Operator version").Info(fmt.Sprintf("%#v", version.Get()))

	mgrOptions := ctrl.Options{
		Client: client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{},
			},
		},
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		WebhookServer: webhook.NewServer(webhook.Options{
			Port: 9443,
			TLSOpts: []func(*tls.Config){func(config *tls.Config) {
				config = &tls.Config{
					MinVersion: tls.VersionTLS12,
				}
			}},
		}),
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "797f9276.open-cluster-management.io",
		LeaseDuration:          &leaseDuration,
		RenewDeadline:          &renewDeadline,
		RetryPeriod:            &retryPeriod,
		Controller: config.Controller{
			CacheSyncTimeout: cacheDuration,
		},
		// LeaderElectionNamespace: "backplane-operator-system", // Ensure this is commented out. Uncomment only for running operator locally.
	}

	setupLog.Info("Disabling Operator Client Cache for high-memory resources")
	mgrOptions.Client.Cache.DisableFor = []client.Object{
		&corev1.Secret{},
		&rbacv1.ClusterRole{},
		&rbacv1.ClusterRoleBinding{},
		&rbacv1.RoleBinding{},
		&corev1.ConfigMap{},
		&corev1.ServiceAccount{},
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOptions)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// use uncached client for setup before manager starts
	uncachedClient, err := client.New(ctrl.GetConfigOrDie(), client.Options{
		Scheme: mgr.GetScheme(),
	})
	if err != nil {
		setupLog.Error(err, "unable to create uncached client")
		os.Exit(1)
	}

	// Force OperatorCondition Upgradeable to False
	//
	// We have to at least default the condition to False or
	// OLM will use the Readiness condition via our readiness probe instead:
	// https://olm.operatorframework.io/docs/advanced-tasks/communicating-operator-conditions-to-olm/#setting-defaults
	//
	// We want to force it to False to ensure that the final decision about whether
	// the operator can be upgraded stays within the mce controller.
	setupLog.Info("Setting OperatorCondition.")
	upgradeableCondition, err := utils.NewOperatorCondition(uncachedClient, operatorsapiv2.Upgradeable)
	ctx := ctrl.SetupSignalHandler()

	if err != nil {
		setupLog.Error(err, "Cannot create the Upgradeable Operator Condition")
		os.Exit(1)
	}
	err = upgradeableCondition.Set(ctx, metav1.ConditionFalse, utils.UpgradeableInitReason, utils.UpgradeableInitMessage)
	if err != nil {
		setupLog.Error(err, "unable to create set operator condition upgradable to false")
		os.Exit(1)
	}

	if err = (&controllers.MultiClusterEngineReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		UncachedClient:  uncachedClient,
		StatusManager:   &status.StatusTracker{Client: mgr.GetClient()},
		UpgradeableCond: upgradeableCondition,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MultiClusterEngine")
		os.Exit(1)
	}

	if !utils.DeployOnOCP() {
		deploymentNamespace, ok := os.LookupEnv("POD_NAMESPACE")
		if !ok {
			setupLog.Info("Failing due to being unable to locate webhook service namespace")
			os.Exit(1)
		}

		kubeClient, err := kubernetes.NewForConfig(ctrl.GetConfigOrDie())
		if err != nil {
			setupLog.Error(err, "unable to new kubeClient")
			os.Exit(1)
		}

		// only cache the resources in deployment namespace to avoid consume large mem.
		informerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClient, 10*time.Minute, informers.WithNamespace(deploymentNamespace))

		certGenerator := servingcert.CertGenerator{
			Namespace:             deploymentNamespace,
			CaBundleConfigmapName: webhookcert.CaBundleConfigmapName,
			SigningKeySecretName:  webhookcert.SigningKeySecretName,
			SignerNamePrefix:      webhookcert.SignerNamePrefix,
			ConfigmapLister:       informerFactory.Core().V1().ConfigMaps().Lister(),
			SecretLister:          informerFactory.Core().V1().Secrets().Lister(),
			Client:                kubeClient,
			EventRecorder:         servingcert.NewEventRecorder(kubeClient, "multicluster-engine-operator", deploymentNamespace),
		}

		if err = (&webhookcert.Reconciler{
			Namespace:     deploymentNamespace,
			CertGenerator: certGenerator,
			Log:           log.Log.WithName(webhookcert.ControllerName),
		}).SetupWithManager(mgr, informerFactory.Core().V1().ConfigMaps().Informer(), informerFactory.Core().V1().Secrets().Informer()); err != nil {
			setupLog.Error(err, "unable to create webhook cert controller", "controller", "MultiClusterEngine")
			os.Exit(1)
		}

		informerFactory.Start(ctx.Done())
		informerFactory.WaitForCacheSync(ctx.Done())

		err = certGenerator.GenerateWebhookCertKey(ctx, webhookcert.MCEWebhookCertSecretName, webhookcert.MCEWebhookServiceName)
		if err != nil {
			setupLog.Error(err, "unable to generate mce webhook cert")
			os.Exit(1)
		}
		err = certGenerator.DumpCertSecret(ctx, webhookcert.MCEWebhookCertSecretName, webhookcert.MCEWebhookCertDir)
		if err != nil {
			setupLog.Error(err, "unable to dump webhook cert secret")
			os.Exit(1)
		}
	}

	// Render CRD templates
	crdsDir := crdsDir

	var backplaneConfig *backplanev1.MultiClusterEngine
	crds, errs := renderer.RenderCRDs(crdsDir, backplaneConfig)
	if len(errs) > 0 {
		for _, err := range errs {
			setupLog.Info(err.Error())
		}
		os.Exit(1)
	}

	// udpate CRDs with retry
	for i := range crds {
		retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			crd := crds[i]
			e := ensureCRD(context.TODO(), uncachedClient, crd)
			return e
		})
		if retryErr != nil {
			setupLog.Error(err, "unable to ensure CRD exists in alloted time. Failing.")
			os.Exit(1)
		}
	}

	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		// https://book.kubebuilder.io/cronjob-tutorial/running.html#running-webhooks-locally, https://book.kubebuilder.io/multiversion-tutorial/webhooks.html#and-maingo
		if err = ensureWebhooks(uncachedClient); err != nil {
			setupLog.Error(err, "unable to ensure webhook", "webhook", "MultiClusterEngine")
			os.Exit(1)
		}

		if err = (&backplanev1.MultiClusterEngine{}).SetupWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "MultiClusterEngine")
			os.Exit(1)
		}
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	multiclusterengineList := &backplanev1.MultiClusterEngineList{}
	err = uncachedClient.List(context.TODO(), multiclusterengineList)
	if err != nil {
		setupLog.Error(err, "Could not set List multicluster engines")
		os.Exit(1)
	}

	if len(multiclusterengineList.Items) == 0 {
		err = upgradeableCondition.Set(ctx, metav1.ConditionTrue, utils.UpgradeableAllowReason, utils.UpgradeableAllowMessage)
		if err != nil {
			setupLog.Error(err, "Could not set Operator Condition")
			os.Exit(1)
		}
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func ensureCRD(ctx context.Context, c client.Client, crd *unstructured.Unstructured) error {
	existingCRD := &unstructured.Unstructured{}
	existingCRD.SetGroupVersionKind(crd.GroupVersionKind())
	err := c.Get(ctx, types.NamespacedName{Name: crd.GetName()}, existingCRD)
	if err != nil && errors.IsNotFound(err) {
		// CRD not found. Create and return
		setupLog.Info(fmt.Sprintf("creating CRD '%s'", crd.GetName()))
		err = c.Create(ctx, crd)
		if err != nil {
			return fmt.Errorf("error creating CRD '%s': %w", crd.GetName(), err)
		}
	} else if err != nil {
		return fmt.Errorf("error getting CRD '%s': %w", crd.GetName(), err)
	} else if err == nil {
		// CRD already exists. Update and return
		if utils.AnnotationPresent(utils.AnnotationMCEIgnore, existingCRD) {
			setupLog.Info(fmt.Sprintf("CRD '%s' has ignore label. Skipping update.", crd.GetName()))
			return nil
		}
		crd.SetResourceVersion(existingCRD.GetResourceVersion())
		setupLog.Info(fmt.Sprintf("updating CRD '%s'", crd.GetName()))
		err = c.Update(ctx, crd)
		if err != nil {
			return fmt.Errorf("error updating CRD '%s': %w", crd.GetName(), err)
		}
	}
	return nil
}

func ensureWebhooks(k8sClient client.Client) error {
	ctx := context.Background()

	deploymentNamespace, ok := os.LookupEnv("POD_NAMESPACE")
	if !ok {
		setupLog.Info("Failing due to being unable to locate webhook service namespace")
		os.Exit(1)
	}

	validatingWebhook := backplanev1.ValidatingWebhook(deploymentNamespace)

	maxAttempts := 10
	for i := 0; i < maxAttempts; i++ {
		setupLog.Info("Applying ValidatingWebhookConfiguration")

		// Get reference to MCE CRD to set as owner of the webhook
		// This way if the CRD is deleted the webhook will be removed with it
		crdKey := types.NamespacedName{Name: crdName}
		owner := &apixv1.CustomResourceDefinition{}
		if err := k8sClient.Get(context.TODO(), crdKey, owner); err != nil {
			setupLog.Error(err, "Failed to get MCE CRD")
			time.Sleep(5 * time.Second)
			continue
		}
		validatingWebhook.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: "apiextensions.k8s.io/v1",
				Kind:       "CustomResourceDefinition",
				Name:       owner.Name,
				UID:        owner.UID,
			},
		})

		if !utils.DeployOnOCP() {
			caBundleConfigmapKey := types.NamespacedName{Name: webhookcert.CaBundleConfigmapName, Namespace: deploymentNamespace}
			caBundleConfigmap := &corev1.ConfigMap{}
			if err := k8sClient.Get(context.TODO(), caBundleConfigmapKey, caBundleConfigmap); err != nil {
				setupLog.Error(err, "Failed to get cabundle configmap")
				time.Sleep(5 * time.Second)
				continue
			}
			cb := caBundleConfigmap.Data["ca-bundle.crt"]
			if len(cb) == 0 {
				setupLog.Error(fmt.Errorf("failed to get cabundle from the configmap"), "")
				time.Sleep(5 * time.Second)
				continue
			}

			for key, _ := range validatingWebhook.Webhooks {
				validatingWebhook.Webhooks[key].ClientConfig.CABundle = []byte(cb)
			}
		}

		existingWebhook := &admissionregistration.ValidatingWebhookConfiguration{}
		existingWebhook.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "admissionregistration.k8s.io",
			Version: "v1",
			Kind:    "ValidatingWebhookConfiguration",
		})
		err := k8sClient.Get(ctx, types.NamespacedName{Name: validatingWebhook.GetName()}, existingWebhook)
		if err != nil && errors.IsNotFound(err) {
			// Webhook not found. Create and return
			err = k8sClient.Create(ctx, validatingWebhook)
			if err != nil {
				setupLog.Error(err, "Error creating validatingwebhookconfiguration")
				time.Sleep(5 * time.Second)
				continue
			}
			return nil
		} else if err != nil {
			setupLog.Error(err, "Error getting validatingwebhookconfiguration")
			time.Sleep(5 * time.Second)
			continue
		} else if err == nil {
			// Webhook already exists. Update and return
			setupLog.Info("Updating existing validatingwebhookconfiguration")
			existingWebhook.Webhooks = validatingWebhook.Webhooks
			err = k8sClient.Update(ctx, existingWebhook)
			if err != nil {
				setupLog.Error(err, "Error updating validatingwebhookconfiguration")
				time.Sleep(5 * time.Second)
				continue
			}
			return nil
		}
	}
	return fmt.Errorf("unable to ensure validatingwebhook exists in allotted time")
}
