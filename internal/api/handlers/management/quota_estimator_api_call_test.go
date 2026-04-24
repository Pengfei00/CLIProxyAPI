package management

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/quotaestimator"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestAPICallRecordsWhamUsageObservationWithoutChangingResponse(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	upstreamBody := `{"plan_type":"pro","windows":[{"window_type":"5h","used_percent":35,"available":true},{"window_type":"7d","used_percent":12,"available":true}]}`

	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:       "codex-oauth-auth",
		Provider: "codex",
		Attributes: map[string]string{
			"plan_type": "pro",
		},
		Metadata: map[string]any{
			"email": "codex@example.com",
		},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	estimator := quotaestimator.New("")
	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
	handler.SetQuotaEstimator(estimator)
	handler.apiCallTransportHook = func(auth *coreauth.Auth) http.RoundTripper {
		return roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodPost {
				t.Fatalf("method = %s, want POST", req.Method)
			}
			if got := req.URL.String(); got != "http://chatgpt.com/backend-api/wham/usage" {
				t.Fatalf("url = %q, want wham usage endpoint", got)
			}
			header := make(http.Header)
			header.Set("Content-Type", "application/json")
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     header,
				Body:       io.NopCloser(strings.NewReader(upstreamBody)),
			}, nil
		})
	}

	body := []byte(`{"auth_index":"` + auth.EnsureIndex() + `","method":"POST","url":"http://chatgpt.com/backend-api/wham/usage"}`)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-call", bytes.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	handler.APICall(ginCtx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}

	var response apiCallResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status_code = %d, want 200", response.StatusCode)
	}
	if response.Body != upstreamBody {
		t.Fatalf("body = %q, want %q", response.Body, upstreamBody)
	}

	detail := estimator.Detail(auth.EnsureIndex(), auth)
	if detail.RecentObservation == nil {
		t.Fatal("expected recent observation to be recorded")
	}
	if got := detail.RecentObservation.PlanType; got != "pro" {
		t.Fatalf("plan_type = %q, want pro", got)
	}
	if len(detail.RecentObservation.Windows) != 2 {
		t.Fatalf("windows len = %d, want 2", len(detail.RecentObservation.Windows))
	}
}

func TestAPICallIgnoresNonWhamUsageRequests(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)

	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:       "codex-oauth-auth",
		Provider: "codex",
		Metadata: map[string]any{
			"email": "codex@example.com",
		},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	estimator := quotaestimator.New("")
	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
	handler.SetQuotaEstimator(estimator)
	handler.apiCallTransportHook = func(auth *coreauth.Auth) http.RoundTripper {
		return roundTripFunc(func(req *http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set("Content-Type", "application/json")
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     header,
				Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			}, nil
		})
	}

	body := []byte(`{"auth_index":"` + auth.EnsureIndex() + `","method":"GET","url":"http://chatgpt.com/backend-api/not-wham"}`)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-call", bytes.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	handler.APICall(ginCtx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}

	detail := estimator.Detail(auth.EnsureIndex(), auth)
	if detail.RecentObservation != nil {
		t.Fatal("expected no recorded observation for non-wham endpoint")
	}
}
