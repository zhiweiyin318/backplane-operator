package webhookcert

import (
	"context"
	"github.com/go-logr/logr"
	"github.com/stolostron/backplane-operator/pkg/servingcert"
	admissionregistration "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

const (
	ControllerName = "webhook-cert-controller"

	CaBundleConfigmapName = "signing-cabundle"
	SigningKeySecretName  = "signing-key"
	SignerNamePrefix      = "multicluster-engine-webhook"

	MCEValidatingWebhookName = "multiclusterengines.multicluster.openshift.io"
	MCEWebhookCertSecretName = "multicluster-engine-operator-webhook"
	MCEWebhookServiceName    = "multicluster-engine-operator-webhook-service"
	MCEWebhookCertDir        = "/tmp/k8s-webhook-server/serving-certs"

	FoundationValidatingWebhookName = "ocm-validating-webhook"
	FoundationMutatingWebhookName   = "ocm-mutating-webhook"
	FoundationWebhookCertSecretName = "ocm-webhook"
	FoundationWebhookServiceName    = "ocm-webhook"
)

// Reconciler reconciles for the webhooks
type Reconciler struct {
	Namespace     string
	CertGenerator servingcert.CertGenerator
	Log           logr.Logger
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (retRes ctrl.Result, retErr error) {
	r.Log.V(2).Info("Reconciling webhook cert controller")

	signingCertKeyPair, err := r.CertGenerator.EnsureSigningCertKeyPair(ctx)
	if err != nil {
		r.Log.Error(err, "failed to sign cert and key")
		return ctrl.Result{}, err
	}
	cabundleCerts, err := r.CertGenerator.EnsureConfigMapCABundle(ctx, signingCertKeyPair)
	if err != nil {
		r.Log.Error(err, "failed to generate configmap ca bundle")
		return ctrl.Result{}, err
	}

	err = r.CertGenerator.EnsureTargetCertKeyPair(ctx, signingCertKeyPair, cabundleCerts,
		MCEWebhookCertSecretName, MCEWebhookServiceName)
	if err != nil {
		r.Log.Error(err, "failed to generate certKey secret multicluster-engine-operator-webhook")
		return ctrl.Result{}, err
	}

	err = r.CertGenerator.EnsureTargetCertKeyPair(ctx, signingCertKeyPair, cabundleCerts,
		FoundationWebhookCertSecretName, FoundationWebhookServiceName)
	if err != nil {
		r.Log.Error(err, "failed to generate certKey secret  ocm-webhook")
		return ctrl.Result{}, err
	}

	err = r.CertGenerator.DumpCertSecret(ctx, MCEWebhookCertSecretName, MCEWebhookCertDir)
	if err != nil {
		r.Log.Error(err, "failed to write certKey into /tmp/k8s-webhook-server/serving-certs")
		return ctrl.Result{}, err
	}

	err = r.CertGenerator.InjectCABundle(ctx,
		[]string{},
		[]string{MCEValidatingWebhookName, FoundationValidatingWebhookName},
		[]string{FoundationMutatingWebhookName})
	if err != nil {
		r.Log.Error(err, "failed to inject caBundle into webhook")
		return ctrl.Result{}, err
	}
	return ctrl.Result{
		Requeue:      true,
		RequeueAfter: 10 * time.Minute,
	}, nil
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, configmapInformer, secretInformer cache.SharedIndexInformer) error {
	return ctrl.NewControllerManagedBy(mgr).Named(ControllerName).
		Watches(&admissionregistration.MutatingWebhookConfiguration{},
			&handler.Funcs{
				CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
					switch e.Object.GetName() {
					case FoundationMutatingWebhookName:
						q.Add(reconcile.Request{NamespacedName: types.NamespacedName{
							Name: e.Object.GetName(),
						}})
					}
				},
				UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
					switch e.ObjectNew.GetName() {
					case FoundationMutatingWebhookName:
						q.Add(reconcile.Request{NamespacedName: types.NamespacedName{
							Name: e.ObjectNew.GetName(),
						}})
					}
				},
			}).
		Watches(&admissionregistration.ValidatingWebhookConfiguration{},
			&handler.Funcs{
				CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.RateLimitingInterface) {
					switch e.Object.GetName() {
					case MCEValidatingWebhookName, FoundationValidatingWebhookName:
						q.Add(reconcile.Request{NamespacedName: types.NamespacedName{
							Name: e.Object.GetName(),
						}})
					}
				},
				UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
					switch e.ObjectNew.GetName() {
					case MCEValidatingWebhookName, FoundationValidatingWebhookName:
						q.Add(reconcile.Request{NamespacedName: types.NamespacedName{
							Name: e.ObjectNew.GetName(),
						}})
					}
				},
			}).
		WatchesRawSource(
			NewConfigmapSource(configmapInformer),
			NewObjectEventHandler(r.Namespace),
			builder.WithPredicates(predicate.Funcs{
				GenericFunc: func(e event.GenericEvent) bool { return false },
				CreateFunc:  func(e event.CreateEvent) bool { return e.Object.GetName() == CaBundleConfigmapName },
				DeleteFunc:  func(e event.DeleteEvent) bool { return e.Object.GetName() == CaBundleConfigmapName },
				UpdateFunc:  func(e event.UpdateEvent) bool { return e.ObjectNew.GetName() == CaBundleConfigmapName },
			}),
		).WatchesRawSource(
		NewSecretSource(secretInformer),
		NewObjectEventHandler(r.Namespace),
		builder.WithPredicates(predicate.Funcs{
			GenericFunc: func(e event.GenericEvent) bool { return false },
			CreateFunc: func(e event.CreateEvent) bool {
				switch e.Object.GetName() {
				case SigningKeySecretName, MCEWebhookCertSecretName, FoundationWebhookCertSecretName:
					return true
				}
				return false
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				switch e.Object.GetName() {
				case SigningKeySecretName, MCEWebhookCertSecretName, FoundationWebhookCertSecretName:
					return true
				}
				return false
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				switch e.ObjectNew.GetName() {
				case SigningKeySecretName, MCEWebhookCertSecretName, FoundationWebhookCertSecretName:
					return true
				}
				return false
			},
		}),
	).Complete(r)
}
