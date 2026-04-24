package quotaestimator

import (
	"context"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestRecordUsageAggregatesPerModel(t *testing.T) {
	t.Parallel()

	estimator := New("")
	auth := testCodexOAuthAuth()
	authIndex := auth.EnsureIndex()
	start := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)

	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start,
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "pro",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 0,
			Available:   true,
		}},
	})
	estimator.RecordUsage(coreusage.Record{
		AuthID:      auth.ID,
		AuthIndex:   authIndex,
		Model:       "gpt-5.4",
		RequestedAt: start.Add(2 * time.Minute),
		Detail: coreusage.Detail{
			InputTokens:     100,
			CachedTokens:    20,
			OutputTokens:    30,
			ReasoningTokens: 5,
			TotalTokens:     135,
		},
	}, auth)
	estimator.RecordUsage(coreusage.Record{
		AuthID:      auth.ID,
		AuthIndex:   authIndex,
		Model:       "gpt-5.4-mini",
		RequestedAt: start.Add(3 * time.Minute),
		Detail: coreusage.Detail{
			InputTokens:     50,
			CachedTokens:    10,
			OutputTokens:    15,
			ReasoningTokens: 3,
			TotalTokens:     68,
		},
	}, auth)

	detail := estimator.Detail(authIndex, auth)
	current := detail.CurrentEstimates["5h"]
	if current.CurrentTokens.ReadTokens != 120 {
		t.Fatalf("read_tokens = %d, want 120", current.CurrentTokens.ReadTokens)
	}
	if current.CurrentTokens.CacheReadTokens != 30 {
		t.Fatalf("cache_read_tokens = %d, want 30", current.CurrentTokens.CacheReadTokens)
	}
	if current.CurrentTokens.OutputTokens != 45 {
		t.Fatalf("output_tokens = %d, want 45", current.CurrentTokens.OutputTokens)
	}
	if current.CurrentTokens.ReasoningTokens != 8 {
		t.Fatalf("reasoning_tokens = %d, want 8", current.CurrentTokens.ReasoningTokens)
	}
	if current.CurrentTokens.TotalTokens != 203 {
		t.Fatalf("total_tokens = %d, want 203", current.CurrentTokens.TotalTokens)
	}
	if got := current.PerModel["gpt-5.4"].ReadTokens; got != 80 {
		t.Fatalf("gpt-5.4 read_tokens = %d, want 80", got)
	}
	if got := current.PerModel["gpt-5.4-mini"].TotalTokens; got != 68 {
		t.Fatalf("gpt-5.4-mini total_tokens = %d, want 68", got)
	}
}

func TestFirstObservationSetsLastRefreshTime(t *testing.T) {
	t.Parallel()

	estimator := New("")
	auth := testCodexOAuthAuth()
	authIndex := auth.EnsureIndex()
	start := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)

	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start,
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "pro",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 30,
			Available:   true,
			ResetAt:     start.Add(5 * time.Hour),
		}},
	})

	detail := estimator.Detail(authIndex, auth)
	if detail.CodexAccountSummary.LastQuotaRefreshAt.IsZero() {
		t.Fatal("expected last_quota_refresh_at to be recorded on first observation")
	}
	if !detail.CodexAccountSummary.LastQuotaRefreshAt.Equal(start) {
		t.Fatalf("last_quota_refresh_at = %v, want %v", detail.CodexAccountSummary.LastQuotaRefreshAt, start)
	}
	if detail.CodexAccountSummary.LastObservationAt.IsZero() {
		t.Fatal("expected last_observation_at to be recorded on first observation")
	}
	current := detail.CurrentEstimates["5h"]
	if current.LastRefreshAt.IsZero() {
		t.Fatal("expected current estimate last_refresh_at to be populated")
	}
}

func TestRecordObservationDetectsRefreshAndClosesSample(t *testing.T) {
	t.Parallel()

	estimator := New("")
	auth := testCodexOAuthAuth()
	authIndex := auth.EnsureIndex()
	start := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)

	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start,
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "pro",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 0,
			Available:   true,
		}},
	})
	estimator.RecordUsage(coreusage.Record{
		AuthID:      auth.ID,
		AuthIndex:   authIndex,
		Model:       "gpt-5.4",
		RequestedAt: start.Add(1 * time.Minute),
		Detail: coreusage.Detail{
			InputTokens:  60,
			OutputTokens: 20,
			TotalTokens:  80,
		},
	}, auth)
	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start.Add(10 * time.Minute),
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "pro",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 45,
			Available:   true,
		}},
	})
	estimator.RecordUsage(coreusage.Record{
		AuthID:      auth.ID,
		AuthIndex:   authIndex,
		Model:       "gpt-5.4",
		RequestedAt: start.Add(11 * time.Minute),
		Detail: coreusage.Detail{
			InputTokens:  40,
			OutputTokens: 10,
			TotalTokens:  50,
		},
	}, auth)
	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start.Add(6 * time.Hour),
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "pro",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 5,
			Available:   true,
		}},
	})

	detail := estimator.Detail(authIndex, auth)
	samples := detail.ClosedSamples["5h"]
	if len(samples) != 1 {
		t.Fatalf("closed samples len = %d, want 1", len(samples))
	}
	if samples[0].CloseReason != closeReasonRefreshedBeforeFull {
		t.Fatalf("close_reason = %q, want %q", samples[0].CloseReason, closeReasonRefreshedBeforeFull)
	}
	if samples[0].Tokens.TotalTokens != 130 {
		t.Fatalf("sample total_tokens = %d, want 130", samples[0].Tokens.TotalTokens)
	}
	current := detail.CurrentEstimates["5h"]
	if current.CycleStartSource != startSourceQuotaRefresh {
		t.Fatalf("start_source = %q, want %q", current.CycleStartSource, startSourceQuotaRefresh)
	}
	if current.CurrentUsedPercent != 5 {
		t.Fatalf("current_used_percent = %v, want 5", current.CurrentUsedPercent)
	}
}

func TestRecordUsageWithContextAutoRefreshKeepsCurrentCycle(t *testing.T) {
	t.Parallel()

	estimator := New("")
	auth := testCodexOAuthAuth()
	authIndex := auth.EnsureIndex()
	start := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)
	refreshObservedAt := start.Add(5 * time.Hour)
	refreshCalls := 0

	estimator.now = func() time.Time {
		return refreshObservedAt
	}
	estimator.SetUsageFetcher(func(ctx context.Context, gotAuth *coreauth.Auth) ([]byte, error) {
		refreshCalls++
		if gotAuth == nil || gotAuth.EnsureIndex() != authIndex {
			t.Fatalf("unexpected auth passed to usage fetcher: %#v", gotAuth)
		}
		return []byte(`{"plan_type":"pro","windows":[{"window_type":"5h","used_percent":80,"available":true,"reset_at":"2026-04-24T10:00:00Z"}]}`), nil
	})

	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start,
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "pro",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 60,
			Available:   true,
			ResetAt:     refreshObservedAt,
		}},
	})

	estimator.RecordUsageWithContext(context.Background(), coreusage.Record{
		AuthID:      auth.ID,
		AuthIndex:   authIndex,
		Model:       "gpt-5.4",
		RequestedAt: refreshObservedAt.Add(2 * time.Minute),
		Detail: coreusage.Detail{
			InputTokens:  60,
			OutputTokens: 20,
			TotalTokens:  80,
		},
	}, auth)

	if refreshCalls != 1 {
		t.Fatalf("usage fetch calls = %d, want 1", refreshCalls)
	}

	detail := estimator.Detail(authIndex, auth)
	if len(detail.ClosedSamples["5h"]) != 0 {
		t.Fatalf("closed samples len = %d, want 0", len(detail.ClosedSamples["5h"]))
	}
	current := detail.CurrentEstimates["5h"]
	if !current.CurrentCycleStartedAt.Equal(start) {
		t.Fatalf("current_cycle_started_at = %v, want %v", current.CurrentCycleStartedAt, start)
	}
	if current.CurrentUsedPercent != 80 {
		t.Fatalf("current_used_percent = %v, want 80", current.CurrentUsedPercent)
	}
	if !current.LastRefreshAt.Equal(refreshObservedAt) {
		t.Fatalf("last_refresh_at = %v, want %v", current.LastRefreshAt, refreshObservedAt)
	}
}

func TestRecordExhaustionThenRefresh(t *testing.T) {
	t.Parallel()

	estimator := New("")
	auth := testCodexOAuthAuth()
	authIndex := auth.EnsureIndex()
	start := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)
	retryAfter := 30 * time.Minute

	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start,
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "pro",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 60,
			Available:   true,
		}},
	})
	estimator.RecordUsage(coreusage.Record{
		AuthID:      auth.ID,
		AuthIndex:   authIndex,
		Model:       "gpt-5.4",
		RequestedAt: start.Add(2 * time.Minute),
		Detail: coreusage.Detail{
			InputTokens:  50,
			OutputTokens: 30,
			TotalTokens:  80,
		},
	}, auth)
	estimator.RecordExhaustionEvent(auth, "gpt-5.4", &retryAfter, start.Add(3*time.Minute))

	detail := estimator.Detail(authIndex, auth)
	current := detail.CurrentEstimates["5h"]
	if current.Confidence != "low" {
		t.Fatalf("confidence = %q, want low", current.Confidence)
	}
	if !current.ExhaustedPending {
		t.Fatal("expected exhausted_pending to be true")
	}

	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start.Add(4 * time.Minute),
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "pro",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 100,
			Available:   false,
		}},
	})
	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start.Add(6 * time.Hour),
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "pro",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 10,
			Available:   true,
		}},
	})

	detail = estimator.Detail(authIndex, auth)
	samples := detail.ClosedSamples["5h"]
	if len(samples) != 1 {
		t.Fatalf("closed samples len = %d, want 1", len(samples))
	}
	if samples[0].CloseReason != closeReasonExhaustedRefreshed {
		t.Fatalf("close_reason = %q, want %q", samples[0].CloseReason, closeReasonExhaustedRefreshed)
	}
	if samples[0].Tokens.TotalTokens != 80 {
		t.Fatalf("sample total_tokens = %d, want 80", samples[0].Tokens.TotalTokens)
	}
	current = detail.CurrentEstimates["5h"]
	if current.CurrentUsedPercent != 10 {
		t.Fatalf("current_used_percent = %v, want 10", current.CurrentUsedPercent)
	}
	if len(detail.Exhaustions) != 1 {
		t.Fatalf("exhaustion events len = %d, want 1", len(detail.Exhaustions))
	}
}

func TestRecordUsageWithContextAutoRefreshStartsNewCycleAfterReset(t *testing.T) {
	t.Parallel()

	estimator := New("")
	auth := testCodexOAuthAuth()
	authIndex := auth.EnsureIndex()
	start := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)
	refreshBoundary := start.Add(5 * time.Hour)
	refreshObservedAt := refreshBoundary.Add(2 * time.Minute)

	estimator.now = func() time.Time {
		return refreshObservedAt
	}
	estimator.SetUsageFetcher(func(ctx context.Context, gotAuth *coreauth.Auth) ([]byte, error) {
		return []byte(`{"plan_type":"pro","windows":[{"window_type":"5h","used_percent":10,"available":true,"reset_at":"2026-04-24T11:00:00Z"}]}`), nil
	})

	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start,
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "pro",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 70,
			Available:   true,
			ResetAt:     refreshBoundary,
		}},
	})
	estimator.RecordUsage(coreusage.Record{
		AuthID:      auth.ID,
		AuthIndex:   authIndex,
		Model:       "gpt-5.4",
		RequestedAt: start.Add(30 * time.Minute),
		Detail: coreusage.Detail{
			InputTokens:  40,
			OutputTokens: 10,
			TotalTokens:  50,
		},
	}, auth)

	estimator.RecordUsageWithContext(context.Background(), coreusage.Record{
		AuthID:      auth.ID,
		AuthIndex:   authIndex,
		Model:       "gpt-5.4",
		RequestedAt: refreshObservedAt.Add(1 * time.Minute),
		Detail: coreusage.Detail{
			InputTokens:  20,
			OutputTokens: 10,
			TotalTokens:  30,
		},
	}, auth)

	detail := estimator.Detail(authIndex, auth)
	samples := detail.ClosedSamples["5h"]
	if len(samples) != 1 {
		t.Fatalf("closed samples len = %d, want 1", len(samples))
	}
	if samples[0].CloseReason != closeReasonRefreshedBeforeFull {
		t.Fatalf("close_reason = %q, want %q", samples[0].CloseReason, closeReasonRefreshedBeforeFull)
	}
	current := detail.CurrentEstimates["5h"]
	if !current.CurrentCycleStartedAt.Equal(refreshObservedAt) {
		t.Fatalf("current_cycle_started_at = %v, want %v", current.CurrentCycleStartedAt, refreshObservedAt)
	}
	if current.CurrentUsedPercent != 10 {
		t.Fatalf("current_used_percent = %v, want 10", current.CurrentUsedPercent)
	}
	if current.CurrentTokens.TotalTokens != 30 {
		t.Fatalf("current total_tokens = %d, want 30", current.CurrentTokens.TotalTokens)
	}
}

func TestRecordObservationSplitsOnPlanTypeChange(t *testing.T) {
	t.Parallel()

	estimator := New("")
	auth := testCodexOAuthAuth()
	authIndex := auth.EnsureIndex()
	start := time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)

	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start,
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "plus",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 10,
			Available:   true,
		}},
	})
	estimator.RecordUsage(coreusage.Record{
		AuthID:      auth.ID,
		AuthIndex:   authIndex,
		Model:       "gpt-5.4",
		RequestedAt: start.Add(1 * time.Minute),
		Detail: coreusage.Detail{
			InputTokens:  20,
			OutputTokens: 20,
			TotalTokens:  40,
		},
	}, auth)
	estimator.RecordObservation(QuotaObservation{
		ObservedAt: start.Add(2 * time.Minute),
		AuthID:     auth.ID,
		AuthIndex:  authIndex,
		PlanType:   "pro",
		Windows: []QuotaWindowObservation{{
			WindowType:  "5h",
			UsedPercent: 12,
			Available:   true,
		}},
	})

	detail := estimator.Detail(authIndex, auth)
	samples := detail.ClosedSamples["5h"]
	if len(samples) != 1 {
		t.Fatalf("closed samples len = %d, want 1", len(samples))
	}
	if samples[0].CloseReason != closeReasonPlanTypeChanged {
		t.Fatalf("close_reason = %q, want %q", samples[0].CloseReason, closeReasonPlanTypeChanged)
	}
	if detail.CodexAccountSummary.PlanType != "pro" {
		t.Fatalf("plan_type = %q, want pro", detail.CodexAccountSummary.PlanType)
	}
}

func testCodexOAuthAuth() *coreauth.Auth {
	return &coreauth.Auth{
		ID:       "codex-oauth-auth",
		Provider: "codex",
		Attributes: map[string]string{
			"plan_type": "pro",
		},
		Metadata: map[string]any{
			"email": "codex@example.com",
		},
	}
}
