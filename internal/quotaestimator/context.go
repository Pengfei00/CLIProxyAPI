package quotaestimator

import (
	"context"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	ginEstimatorKey    = "quota_estimator"
	ginSelectedAuthKey = "quota_estimator_selected_auth"
)

type estimatorContextKey struct{}

// WithContext stores the estimator directly on a context.Context.
func WithContext(ctx context.Context, estimator *Estimator) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if estimator == nil {
		return ctx
	}
	return context.WithValue(ctx, estimatorContextKey{}, estimator)
}

// AttachToGin stores the estimator on the request context so downstream hooks can reuse it.
func AttachToGin(c *gin.Context, estimator *Estimator) {
	if c == nil || estimator == nil {
		return
	}
	c.Set(ginEstimatorKey, estimator)
	if c.Request != nil {
		ctx := WithContext(c.Request.Context(), estimator)
		c.Request = c.Request.WithContext(ctx)
	}
}

// FromContext resolves the estimator from a request context.
func FromContext(ctx context.Context) *Estimator {
	if ctx == nil {
		return nil
	}
	if estimator, ok := ctx.Value(estimatorContextKey{}).(*Estimator); ok && estimator != nil {
		return estimator
	}
	ginCtx, ok := ctx.Value("gin").(*gin.Context)
	if !ok || ginCtx == nil {
		return nil
	}
	raw, exists := ginCtx.Get(ginEstimatorKey)
	if !exists {
		return nil
	}
	estimator, _ := raw.(*Estimator)
	return estimator
}

// SetSelectedAuth stores the selected auth snapshot on the current Gin context.
func SetSelectedAuth(c *gin.Context, auth *coreauth.Auth) {
	if c == nil || auth == nil {
		return
	}
	c.Set(ginSelectedAuthKey, auth.Clone())
}

// SelectedAuthFromContext resolves the selected auth snapshot from a request context.
func SelectedAuthFromContext(ctx context.Context) *coreauth.Auth {
	if ctx == nil {
		return nil
	}
	ginCtx, ok := ctx.Value("gin").(*gin.Context)
	if !ok || ginCtx == nil {
		return nil
	}
	raw, exists := ginCtx.Get(ginSelectedAuthKey)
	if !exists {
		return nil
	}
	auth, _ := raw.(*coreauth.Auth)
	if auth == nil {
		return nil
	}
	return auth.Clone()
}
