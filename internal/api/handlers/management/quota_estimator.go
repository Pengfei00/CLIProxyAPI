package management

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/quotaestimator"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type exhaustionEventResponse struct {
	ObservedAt         *time.Time `json:"observed_at,omitempty"`
	AuthID             string     `json:"auth_id,omitempty"`
	AuthIndex          string     `json:"auth_index,omitempty"`
	PlanType           string     `json:"plan_type,omitempty"`
	Model              string     `json:"model,omitempty"`
	RetryAfterSeconds  int64      `json:"retry_after_seconds,omitempty"`
	RetryAfterDeadline *time.Time `json:"retry_after_deadline,omitempty"`
}

type currentCycleEstimateResponse struct {
	AuthID                string                                 `json:"auth_id,omitempty"`
	AuthIndex             string                                 `json:"auth_index,omitempty"`
	PlanType              string                                 `json:"plan_type,omitempty"`
	WindowType            string                                 `json:"window_type,omitempty"`
	CurrentCycleStartedAt *time.Time                             `json:"current_cycle_started_at,omitempty"`
	CycleStartSource      string                                 `json:"cycle_start_source,omitempty"`
	LastRefreshAt         *time.Time                             `json:"last_refresh_at,omitempty"`
	CurrentUsedPercent    float64                                `json:"current_used_percent"`
	CurrentTokens         quotaestimator.TokenSummary            `json:"current_tokens"`
	PerModel              map[string]quotaestimator.TokenSummary `json:"per_model,omitempty"`
	EstimatedCapacity     quotaestimator.TokenSummary            `json:"estimated_capacity"`
	SampleCount           int                                    `json:"sample_count"`
	Confidence            string                                 `json:"confidence,omitempty"`
	LastExhaustionAt      *time.Time                             `json:"last_exhaustion_at,omitempty"`
	ExhaustedPending      bool                                   `json:"exhausted_pending"`
	ExhaustedConfirmed    bool                                   `json:"exhausted_confirmed"`
}

type codexAccountSummaryResponse struct {
	AuthID             string                         `json:"auth_id,omitempty"`
	AuthIndex          string                         `json:"auth_index,omitempty"`
	AccountType        string                         `json:"account_type,omitempty"`
	Account            string                         `json:"account,omitempty"`
	PlanType           string                         `json:"plan_type,omitempty"`
	LastQuotaRefreshAt *time.Time                     `json:"last_quota_refresh_at,omitempty"`
	LastObservationAt  *time.Time                     `json:"last_observation_at,omitempty"`
	LastExhaustion     *exhaustionEventResponse       `json:"last_exhaustion_event,omitempty"`
	Windows            []currentCycleEstimateResponse `json:"windows,omitempty"`
}

type codexAccountDetailResponse struct {
	codexAccountSummaryResponse
	RecentObservation *quotaestimator.QuotaObservation             `json:"recent_observation,omitempty"`
	ClosedSamples     map[string][]quotaestimator.QuotaCycleSample `json:"closed_samples,omitempty"`
	Observations      []quotaestimator.QuotaObservation            `json:"observations,omitempty"`
	Exhaustions       []exhaustionEventResponse                    `json:"exhaustion_events,omitempty"`
	OpenCycles        map[string]quotaestimator.OpenQuotaCycle     `json:"open_cycles,omitempty"`
	CurrentEstimates  map[string]currentCycleEstimateResponse      `json:"current_estimates,omitempty"`
}

// GetCodexQuotaEstimator returns the current quota-cycle summaries for all Codex OAuth auths.
func (h *Handler) GetCodexQuotaEstimator(c *gin.Context) {
	if h == nil || h.quotaEstimator == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "quota estimator unavailable"})
		return
	}
	summaries := h.quotaEstimator.Summaries(h.listAuths())
	accounts := make([]codexAccountSummaryResponse, 0, len(summaries))
	for _, summary := range summaries {
		accounts = append(accounts, buildCodexAccountSummaryResponse(summary))
	}
	c.JSON(http.StatusOK, gin.H{
		"accounts": accounts,
	})
}

// GetCodexQuotaEstimatorByAuth returns the detailed quota-cycle view for one Codex auth.
func (h *Handler) GetCodexQuotaEstimatorByAuth(c *gin.Context) {
	if h == nil || h.quotaEstimator == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "quota estimator unavailable"})
		return
	}
	authIndex := strings.TrimSpace(c.Param("authIndex"))
	if authIndex == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing authIndex"})
		return
	}
	c.JSON(http.StatusOK, buildCodexAccountDetailResponse(h.quotaEstimator.Detail(authIndex, h.authByIndex(authIndex))))
}

func (h *Handler) listAuths() []*coreauth.Auth {
	if h == nil || h.authManager == nil {
		return nil
	}
	return h.authManager.List()
}

func buildCodexAccountSummaryResponse(summary quotaestimator.CodexAccountSummary) codexAccountSummaryResponse {
	response := codexAccountSummaryResponse{
		AuthID:             summary.AuthID,
		AuthIndex:          summary.AuthIndex,
		AccountType:        summary.AccountType,
		Account:            summary.Account,
		PlanType:           summary.PlanType,
		LastQuotaRefreshAt: nilIfZero(summary.LastQuotaRefreshAt),
		LastObservationAt:  nilIfZero(summary.LastObservationAt),
		LastExhaustion:     buildExhaustionEventResponse(summary.LastExhaustion),
	}
	if len(summary.Windows) > 0 {
		response.Windows = make([]currentCycleEstimateResponse, 0, len(summary.Windows))
		for _, window := range summary.Windows {
			response.Windows = append(response.Windows, buildCurrentCycleEstimateResponse(window))
		}
	}
	return response
}

func buildCodexAccountDetailResponse(detail quotaestimator.CodexAccountDetail) codexAccountDetailResponse {
	response := codexAccountDetailResponse{
		codexAccountSummaryResponse: buildCodexAccountSummaryResponse(detail.CodexAccountSummary),
		RecentObservation:           detail.RecentObservation,
		ClosedSamples:               detail.ClosedSamples,
		Observations:                detail.Observations,
		OpenCycles:                  detail.OpenCycles,
	}
	if len(detail.Exhaustions) > 0 {
		response.Exhaustions = make([]exhaustionEventResponse, 0, len(detail.Exhaustions))
		for _, event := range detail.Exhaustions {
			response.Exhaustions = append(response.Exhaustions, buildExhaustionEventValueResponse(event))
		}
	}
	if len(detail.CurrentEstimates) > 0 {
		response.CurrentEstimates = make(map[string]currentCycleEstimateResponse, len(detail.CurrentEstimates))
		for key, estimate := range detail.CurrentEstimates {
			response.CurrentEstimates[key] = buildCurrentCycleEstimateResponse(estimate)
		}
	}
	return response
}

func buildCurrentCycleEstimateResponse(estimate quotaestimator.CurrentCycleEstimate) currentCycleEstimateResponse {
	return currentCycleEstimateResponse{
		AuthID:                estimate.AuthID,
		AuthIndex:             estimate.AuthIndex,
		PlanType:              estimate.PlanType,
		WindowType:            estimate.WindowType,
		CurrentCycleStartedAt: nilIfZero(estimate.CurrentCycleStartedAt),
		CycleStartSource:      estimate.CycleStartSource,
		LastRefreshAt:         nilIfZero(estimate.LastRefreshAt),
		CurrentUsedPercent:    estimate.CurrentUsedPercent,
		CurrentTokens:         estimate.CurrentTokens,
		PerModel:              estimate.PerModel,
		EstimatedCapacity:     estimate.EstimatedCapacity,
		SampleCount:           estimate.SampleCount,
		Confidence:            estimate.Confidence,
		LastExhaustionAt:      nilIfZero(estimate.LastExhaustionAt),
		ExhaustedPending:      estimate.ExhaustedPending,
		ExhaustedConfirmed:    estimate.ExhaustedConfirmed,
	}
}

func buildExhaustionEventResponse(event *quotaestimator.ExhaustionEvent) *exhaustionEventResponse {
	if event == nil {
		return nil
	}
	response := buildExhaustionEventValueResponse(*event)
	return &response
}

func buildExhaustionEventValueResponse(event quotaestimator.ExhaustionEvent) exhaustionEventResponse {
	return exhaustionEventResponse{
		ObservedAt:         nilIfZero(event.ObservedAt),
		AuthID:             event.AuthID,
		AuthIndex:          event.AuthIndex,
		PlanType:           event.PlanType,
		Model:              event.Model,
		RetryAfterSeconds:  event.RetryAfterSeconds,
		RetryAfterDeadline: nilIfZero(event.RetryAfterDeadline),
	}
}

func nilIfZero(ts time.Time) *time.Time {
	if ts.IsZero() {
		return nil
	}
	value := ts.UTC()
	return &value
}
