package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/quotaestimator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestRecordCodexQuotaExhaustion(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	estimator := quotaestimator.New("")
	quotaestimator.AttachToGin(ginCtx, estimator)
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	auth := &cliproxyauth.Auth{
		ID:       "codex-oauth-auth",
		Provider: "codex",
		Metadata: map[string]any{
			"email": "codex@example.com",
		},
	}

	recordCodexQuotaExhaustion(ctx, auth, "gpt-5.4", http.StatusTooManyRequests, []byte(`{"error":{"type":"usage_limit_reached","resets_in_seconds":120}}`), time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC))

	detail := estimator.Detail(auth.EnsureIndex(), auth)
	if len(detail.Exhaustions) != 1 {
		t.Fatalf("exhaustion events len = %d, want 1", len(detail.Exhaustions))
	}
	if detail.Exhaustions[0].Model != "gpt-5.4" {
		t.Fatalf("model = %q, want gpt-5.4", detail.Exhaustions[0].Model)
	}
}

func TestRecordCodexQuotaExhaustionIgnoresNonUsageLimit(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	estimator := quotaestimator.New("")
	quotaestimator.AttachToGin(ginCtx, estimator)
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	auth := &cliproxyauth.Auth{
		ID:       "codex-oauth-auth",
		Provider: "codex",
		Metadata: map[string]any{
			"email": "codex@example.com",
		},
	}

	recordCodexQuotaExhaustion(ctx, auth, "gpt-5.4", http.StatusTooManyRequests, []byte(`{"error":{"type":"server_error","resets_in_seconds":120}}`), time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC))

	detail := estimator.Detail(auth.EnsureIndex(), auth)
	if len(detail.Exhaustions) != 0 {
		t.Fatalf("exhaustion events len = %d, want 0", len(detail.Exhaustions))
	}
}

func TestRecordCodexQuotaExhaustionWithoutRetryAfterStillRecordsEvent(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	estimator := quotaestimator.New("")
	quotaestimator.AttachToGin(ginCtx, estimator)
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	auth := &cliproxyauth.Auth{
		ID:       "codex-oauth-auth",
		Provider: "codex",
		Metadata: map[string]any{
			"email": "codex@example.com",
		},
	}

	recordCodexQuotaExhaustion(ctx, auth, "gpt-5.4", http.StatusTooManyRequests, []byte(`{"error":{"type":"usage_limit_reached"}}`), time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC))

	detail := estimator.Detail(auth.EnsureIndex(), auth)
	if len(detail.Exhaustions) != 1 {
		t.Fatalf("exhaustion events len = %d, want 1", len(detail.Exhaustions))
	}
	if detail.Exhaustions[0].RetryAfterSeconds != 0 {
		t.Fatalf("retry_after_seconds = %d, want 0", detail.Exhaustions[0].RetryAfterSeconds)
	}
}
