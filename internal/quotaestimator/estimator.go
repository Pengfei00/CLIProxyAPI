package quotaestimator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

const (
	stateVersion                   = 1
	maxObservationHistoryPerAuth   = 24
	maxExhaustionEventsPerAuth     = 24
	maxClosedSamplesPerBucket      = 48
	observationNoisePercent        = 0.5
	exhaustedThresholdPercent      = 99.0
	defaultPersistDebounce         = 750 * time.Millisecond
	autoRefreshMinInterval         = 30 * time.Second
	closeReasonQuotaRefreshed      = "quota_refreshed"
	closeReasonRefreshedBeforeFull = "refreshed_before_exhausted"
	closeReasonExhaustedRefreshed  = "exhausted_then_refreshed"
	closeReasonPlanTypeChanged     = "plan_type_changed"
	startSourceQuotaRefresh        = "quota_refresh"
	startSourceObservationSeed     = "observation_bootstrap"
	startSourceObservationUsage    = "observation_bootstrap_with_usage"
)

type TokenSummary struct {
	ReadTokens      int64 `json:"read_tokens"`
	CacheReadTokens int64 `json:"cache_read_tokens"`
	OutputTokens    int64 `json:"output_tokens"`
	ReasoningTokens int64 `json:"reasoning_tokens"`
	TotalTokens     int64 `json:"total_tokens"`
}

type QuotaWindowObservation struct {
	WindowType      string    `json:"window_type"`
	UsedPercent     float64   `json:"used_percent"`
	Available       bool      `json:"available"`
	ResetAt         time.Time `json:"reset_at,omitempty"`
	ResetIdentifier string    `json:"reset_identifier,omitempty"`
}

type QuotaObservation struct {
	ObservedAt time.Time                `json:"observed_at"`
	AuthID     string                   `json:"auth_id"`
	AuthIndex  string                   `json:"auth_index"`
	PlanType   string                   `json:"plan_type"`
	Windows    []QuotaWindowObservation `json:"windows"`
	RawSummary string                   `json:"raw_summary,omitempty"`
}

type OpenQuotaCycle struct {
	AuthID             string                  `json:"auth_id"`
	AuthIndex          string                  `json:"auth_index"`
	PlanType           string                  `json:"plan_type"`
	WindowType         string                  `json:"window_type"`
	StartedAt          time.Time               `json:"started_at"`
	StartSource        string                  `json:"start_source"`
	LastRefreshAt      time.Time               `json:"last_refresh_at,omitempty"`
	NextRefreshAt      time.Time               `json:"next_refresh_at,omitempty"`
	StartUsedPercent   float64                 `json:"start_used_percent"`
	CurrentUsedPercent float64                 `json:"current_used_percent"`
	LastObservedAt     time.Time               `json:"last_observed_at,omitempty"`
	Tokens             TokenSummary            `json:"tokens"`
	PerModel           map[string]TokenSummary `json:"per_model,omitempty"`
	LastExhaustionAt   time.Time               `json:"last_exhaustion_at,omitempty"`
	ExhaustedPending   bool                    `json:"exhausted_pending"`
	ExhaustedConfirmed bool                    `json:"exhausted_confirmed"`
	ResetIdentifier    string                  `json:"reset_identifier,omitempty"`
}

type QuotaCycleSample struct {
	AuthID           string                  `json:"auth_id"`
	AuthIndex        string                  `json:"auth_index"`
	CycleStartAt     time.Time               `json:"cycle_start_at"`
	CycleEndAt       time.Time               `json:"cycle_end_at"`
	PlanType         string                  `json:"plan_type"`
	WindowType       string                  `json:"window_type"`
	StartUsedPercent float64                 `json:"start_used_percent"`
	EndUsedPercent   float64                 `json:"end_used_percent"`
	CloseReason      string                  `json:"close_reason"`
	Tokens           TokenSummary            `json:"tokens"`
	PerModel         map[string]TokenSummary `json:"per_model,omitempty"`
}

type ExhaustionEvent struct {
	ObservedAt         time.Time `json:"observed_at"`
	AuthID             string    `json:"auth_id"`
	AuthIndex          string    `json:"auth_index"`
	PlanType           string    `json:"plan_type"`
	Model              string    `json:"model,omitempty"`
	RetryAfterSeconds  int64     `json:"retry_after_seconds,omitempty"`
	RetryAfterDeadline time.Time `json:"retry_after_deadline,omitempty"`
}

type CurrentCycleEstimate struct {
	AuthID                string                  `json:"auth_id"`
	AuthIndex             string                  `json:"auth_index"`
	PlanType              string                  `json:"plan_type"`
	WindowType            string                  `json:"window_type"`
	CurrentCycleStartedAt time.Time               `json:"current_cycle_started_at"`
	CycleStartSource      string                  `json:"cycle_start_source"`
	LastRefreshAt         time.Time               `json:"last_refresh_at,omitempty"`
	CurrentUsedPercent    float64                 `json:"current_used_percent"`
	CurrentTokens         TokenSummary            `json:"current_tokens"`
	PerModel              map[string]TokenSummary `json:"per_model,omitempty"`
	EstimatedCapacity     TokenSummary            `json:"estimated_capacity"`
	SampleCount           int                     `json:"sample_count"`
	Confidence            string                  `json:"confidence"`
	LastExhaustionAt      time.Time               `json:"last_exhaustion_at,omitempty"`
	ExhaustedPending      bool                    `json:"exhausted_pending"`
	ExhaustedConfirmed    bool                    `json:"exhausted_confirmed"`
}

type CodexAccountSummary struct {
	AuthID             string                 `json:"auth_id"`
	AuthIndex          string                 `json:"auth_index"`
	AccountType        string                 `json:"account_type,omitempty"`
	Account            string                 `json:"account,omitempty"`
	PlanType           string                 `json:"plan_type,omitempty"`
	LastQuotaRefreshAt time.Time              `json:"last_quota_refresh_at,omitempty"`
	LastObservationAt  time.Time              `json:"last_observation_at,omitempty"`
	LastExhaustion     *ExhaustionEvent       `json:"last_exhaustion_event,omitempty"`
	Windows            []CurrentCycleEstimate `json:"windows"`
}

type CodexAccountDetail struct {
	CodexAccountSummary
	RecentObservation *QuotaObservation               `json:"recent_observation,omitempty"`
	ClosedSamples     map[string][]QuotaCycleSample   `json:"closed_samples,omitempty"`
	Observations      []QuotaObservation              `json:"observations,omitempty"`
	Exhaustions       []ExhaustionEvent               `json:"exhaustion_events,omitempty"`
	OpenCycles        map[string]OpenQuotaCycle       `json:"open_cycles,omitempty"`
	CurrentEstimates  map[string]CurrentCycleEstimate `json:"current_estimates,omitempty"`
}

type pendingUsage struct {
	AuthID     string                  `json:"auth_id"`
	AuthIndex  string                  `json:"auth_index"`
	PlanType   string                  `json:"plan_type"`
	StartedAt  time.Time               `json:"started_at"`
	Tokens     TokenSummary            `json:"tokens"`
	PerModel   map[string]TokenSummary `json:"per_model,omitempty"`
	LastSeenAt time.Time               `json:"last_seen_at,omitempty"`
}

type estimatorState struct {
	Observations    map[string][]QuotaObservation     `json:"observations"`
	OpenCycles      map[string]OpenQuotaCycle         `json:"open_cycles"`
	ClosedSamples   map[string][]QuotaCycleSample     `json:"closed_samples"`
	Exhaustions     map[string][]ExhaustionEvent      `json:"exhaustion_events"`
	PendingUsage    map[string]pendingUsage           `json:"pending_usage"`
	CurrentEstimate map[string][]CurrentCycleEstimate `json:"current_estimates"`
}

type persistedState struct {
	Version int `json:"version"`
	estimatorState
}

// UsageFetcher fetches the latest wham/usage snapshot for one Codex auth.
type UsageFetcher func(ctx context.Context, auth *coreauth.Auth) ([]byte, error)

// Estimator tracks Codex quota-cycle observations, open windows, and historical samples.
type Estimator struct {
	mu                     sync.RWMutex
	path                   string
	now                    func() time.Time
	persistDebounce        time.Duration
	persistTimer           *time.Timer
	persistDirty           bool
	closed                 bool
	usageFetcher           UsageFetcher
	autoRefreshMinInterval time.Duration
	autoRefreshAttempt     map[string]time.Time
	autoRefreshGroup       singleflight.Group
	state                  estimatorState
}

// New creates a new estimator and loads previously persisted state when available.
func New(path string) *Estimator {
	estimator := &Estimator{
		path:                   strings.TrimSpace(path),
		now:                    time.Now,
		persistDebounce:        defaultPersistDebounce,
		autoRefreshMinInterval: autoRefreshMinInterval,
		autoRefreshAttempt:     make(map[string]time.Time),
	}
	estimator.state = newEstimatorState()
	if err := estimator.load(); err != nil {
		log.WithError(err).Warn("quota estimator: failed to load state")
	}
	return estimator
}

// SetUsageFetcher configures the synchronous wham/usage fetcher used for auto-refresh checks.
func (e *Estimator) SetUsageFetcher(fetcher UsageFetcher) {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.usageFetcher = fetcher
	e.mu.Unlock()
}

func newEstimatorState() estimatorState {
	return estimatorState{
		Observations:    make(map[string][]QuotaObservation),
		OpenCycles:      make(map[string]OpenQuotaCycle),
		ClosedSamples:   make(map[string][]QuotaCycleSample),
		Exhaustions:     make(map[string][]ExhaustionEvent),
		PendingUsage:    make(map[string]pendingUsage),
		CurrentEstimate: make(map[string][]CurrentCycleEstimate),
	}
}

// ResolveStatePath returns the default sidecar path used by the estimator.
func ResolveStatePath(configFilePath string, cfg *config.Config) string {
	if trimmed := strings.TrimSpace(configFilePath); trimmed != "" {
		return filepath.Join(filepath.Dir(trimmed), "quota-estimator-codex.json")
	}
	if cfg != nil {
		if authDir := strings.TrimSpace(cfg.AuthDir); authDir != "" {
			return filepath.Join(authDir, "quota-estimator-codex.json")
		}
	}
	return "quota-estimator-codex.json"
}

// Close flushes any pending sidecar writes.
func (e *Estimator) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	if e.persistTimer != nil {
		e.persistTimer.Stop()
		e.persistTimer = nil
	}
	snapshot := e.cloneStateLocked()
	dirty := e.persistDirty
	e.persistDirty = false
	e.mu.Unlock()
	if !dirty {
		return nil
	}
	return e.save(snapshot)
}

// RecordObservation parses and records a wham/usage observation.
func (e *Estimator) RecordObservation(observation QuotaObservation) {
	if e == nil {
		return
	}
	observation.AuthID = strings.TrimSpace(observation.AuthID)
	observation.AuthIndex = strings.TrimSpace(observation.AuthIndex)
	observation.PlanType = normalizePlanType(observation.PlanType)
	observation.ObservedAt = normalizeTime(observation.ObservedAt)
	if observation.AuthIndex == "" || observation.ObservedAt.IsZero() || len(observation.Windows) == 0 {
		return
	}
	for i := range observation.Windows {
		observation.Windows[i] = normalizeWindowObservation(observation.Windows[i])
	}
	observation.Windows = dedupeWindows(observation.Windows)
	if len(observation.Windows) == 0 {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureStateLocked()

	previousObservation, hasPreviousObservation := lastObservation(e.state.Observations[observation.AuthIndex])
	if hasPreviousObservation && previousObservation.PlanType != "" && observation.PlanType != "" && previousObservation.PlanType != observation.PlanType {
		e.closeAllOpenCyclesLocked(observation.AuthIndex, observation.ObservedAt, closeReasonPlanTypeChanged)
	}

	previousWindows := make(map[string]QuotaWindowObservation, len(previousObservation.Windows))
	for _, window := range previousObservation.Windows {
		previousWindows[window.WindowType] = window
	}

	pending, hasPending := e.state.PendingUsage[observation.AuthIndex]
	openedAny := false
	for _, window := range observation.Windows {
		key := openCycleKey(observation.AuthIndex, window.WindowType)
		cycle, exists := e.state.OpenCycles[key]
		prevWindow, hasPrevWindow := previousWindows[window.WindowType]
		refreshed := false
		if exists && shouldRefreshCycle(cycle, prevWindow, hasPrevWindow, window) {
			closeReason := closeReasonFromTransition(cycle, prevWindow, hasPrevWindow)
			endPercent := cycle.CurrentUsedPercent
			if hasPrevWindow {
				endPercent = prevWindow.UsedPercent
			}
			e.closeCycleLocked(key, cycle, observation.ObservedAt, endPercent, closeReason)
			refreshed = true
			exists = false
		}
		if !exists {
			startSource := startSourceObservationSeed
			if refreshed {
				startSource = startSourceQuotaRefresh
			}
			if hasPending {
				startSource = startSourceObservationUsage
			}
			cycle = OpenQuotaCycle{
				AuthID:             firstNonEmpty(observation.AuthID, pending.AuthID),
				AuthIndex:          observation.AuthIndex,
				PlanType:           firstNonEmpty(observation.PlanType, pending.PlanType),
				WindowType:         window.WindowType,
				StartedAt:          observation.ObservedAt,
				StartSource:        startSource,
				LastRefreshAt:      observation.ObservedAt,
				NextRefreshAt:      window.ResetAt,
				StartUsedPercent:   window.UsedPercent,
				CurrentUsedPercent: window.UsedPercent,
				LastObservedAt:     observation.ObservedAt,
				PerModel:           make(map[string]TokenSummary),
				ResetIdentifier:    window.ResetIdentifier,
			}
			if hasPending {
				applyPendingUsage(&cycle, pending)
				if pending.StartedAt.Before(cycle.StartedAt) && !pending.StartedAt.IsZero() {
					cycle.StartedAt = pending.StartedAt
				}
			}
			e.state.OpenCycles[key] = cycle
			openedAny = true
			continue
		}

		cycle.AuthID = firstNonEmpty(cycle.AuthID, observation.AuthID)
		cycle.PlanType = firstNonEmpty(observation.PlanType, cycle.PlanType)
		cycle.LastRefreshAt = observation.ObservedAt
		cycle.CurrentUsedPercent = window.UsedPercent
		cycle.LastObservedAt = observation.ObservedAt
		if !window.ResetAt.IsZero() {
			cycle.NextRefreshAt = window.ResetAt
		}
		cycle.ResetIdentifier = firstNonEmpty(window.ResetIdentifier, cycle.ResetIdentifier)
		if cycle.ExhaustedPending && (window.UsedPercent >= exhaustedThresholdPercent || !window.Available) {
			cycle.ExhaustedConfirmed = true
		}
		e.state.OpenCycles[key] = cycle
	}

	if hasPending && openedAny {
		delete(e.state.PendingUsage, observation.AuthIndex)
	}

	history := append(cloneObservations(e.state.Observations[observation.AuthIndex]), observation)
	history = keepTail(history, maxObservationHistoryPerAuth)
	e.state.Observations[observation.AuthIndex] = history
	e.recomputeCurrentEstimatesLocked(observation.AuthIndex)
	if !e.hasDueRefreshLocked(observation.AuthIndex, observation.ObservedAt) {
		delete(e.autoRefreshAttempt, observation.AuthIndex)
	}
	e.schedulePersistLocked()
}

// RecordObservationFromBody parses a raw wham/usage response and records it.
func (e *Estimator) RecordObservationFromBody(auth *coreauth.Auth, raw []byte, observedAt time.Time) error {
	observation, err := ParseObservation(raw, auth, observedAt)
	if err != nil {
		return err
	}
	e.RecordObservation(observation)
	return nil
}

// RecordUsage adds request tokens into the currently open cycle for the selected Codex auth.
func (e *Estimator) RecordUsage(record coreusage.Record, auth *coreauth.Auth) {
	e.RecordUsageWithContext(context.Background(), record, auth)
}

// RecordUsageWithContext adds request tokens and performs auto-refresh checks using the request context.
func (e *Estimator) RecordUsageWithContext(ctx context.Context, record coreusage.Record, auth *coreauth.Auth) {
	if e == nil || !IsCodexOAuthAuth(auth) {
		return
	}
	summary := tokenSummaryFromDetail(record.Detail)
	if summary.TotalTokens == 0 && summary.ReadTokens == 0 && summary.CacheReadTokens == 0 && summary.OutputTokens == 0 && summary.ReasoningTokens == 0 {
		return
	}

	authID := strings.TrimSpace(record.AuthID)
	if authID == "" {
		authID = strings.TrimSpace(auth.ID)
	}
	authIndex := strings.TrimSpace(record.AuthIndex)
	if authIndex == "" {
		authIndex = auth.EnsureIndex()
	}
	if authIndex == "" {
		return
	}
	model := strings.TrimSpace(record.Model)
	if model == "" {
		model = "unknown"
	}
	recordedAt := normalizeTime(record.RequestedAt)
	if recordedAt.IsZero() {
		recordedAt = normalizeTime(e.now())
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if e.shouldAutoRefreshUsage(authIndex, recordedAt) {
		e.refreshObservationForUsage(ctx, auth.Clone(), authIndex, recordedAt)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureStateLocked()

	keys := e.openCycleKeysForAuthLocked(authIndex)
	if len(keys) == 0 {
		pending := e.state.PendingUsage[authIndex]
		pending.AuthID = firstNonEmpty(authID, pending.AuthID)
		pending.AuthIndex = authIndex
		pending.PlanType = firstNonEmpty(normalizePlanType(auth.Attributes["plan_type"]), pending.PlanType)
		if pending.PerModel == nil {
			pending.PerModel = make(map[string]TokenSummary)
		}
		if pending.StartedAt.IsZero() || (!recordedAt.IsZero() && recordedAt.Before(pending.StartedAt)) {
			pending.StartedAt = recordedAt
		}
		if recordedAt.After(pending.LastSeenAt) {
			pending.LastSeenAt = recordedAt
		}
		addTokenSummary(&pending.Tokens, summary)
		addModelUsage(pending.PerModel, model, summary)
		e.state.PendingUsage[authIndex] = pending
		e.schedulePersistLocked()
		return
	}

	for _, key := range keys {
		cycle := e.state.OpenCycles[key]
		cycle.AuthID = firstNonEmpty(cycle.AuthID, authID)
		cycle.PlanType = firstNonEmpty(cycle.PlanType, normalizePlanType(auth.Attributes["plan_type"]))
		if cycle.PerModel == nil {
			cycle.PerModel = make(map[string]TokenSummary)
		}
		if cycle.StartedAt.IsZero() || (!recordedAt.IsZero() && recordedAt.Before(cycle.StartedAt) && cycle.StartSource != startSourceQuotaRefresh) {
			cycle.StartedAt = recordedAt
		}
		addTokenSummary(&cycle.Tokens, summary)
		addModelUsage(cycle.PerModel, model, summary)
		e.state.OpenCycles[key] = cycle
	}
	e.recomputeCurrentEstimatesLocked(authIndex)
	e.schedulePersistLocked()
}

func (e *Estimator) shouldAutoRefreshUsage(authIndex string, recordedAt time.Time) bool {
	if e == nil || authIndex == "" || recordedAt.IsZero() {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.usageFetcher == nil {
		return false
	}
	e.ensureStateLocked()
	if !e.hasDueRefreshLocked(authIndex, recordedAt) {
		return false
	}
	if lastAttempt := e.autoRefreshAttempt[authIndex]; !lastAttempt.IsZero() &&
		recordedAt.Before(lastAttempt.Add(e.autoRefreshMinInterval)) {
		return false
	}
	e.autoRefreshAttempt[authIndex] = recordedAt
	return true
}

func (e *Estimator) refreshObservationForUsage(ctx context.Context, auth *coreauth.Auth, authIndex string, recordedAt time.Time) {
	if e == nil || auth == nil || authIndex == "" {
		return
	}
	_, err, _ := e.autoRefreshGroup.Do(authIndex, func() (any, error) {
		if !e.hasDueRefresh(authIndex, recordedAt) {
			return nil, nil
		}
		body, errFetch := e.fetchUsageSnapshot(ctx, auth)
		if errFetch != nil {
			return nil, errFetch
		}
		observedAt := normalizeTime(e.now())
		if observedAt.IsZero() {
			observedAt = recordedAt
		}
		if observedAt.IsZero() {
			observedAt = normalizeTime(time.Now())
		}
		if errRecord := e.RecordObservationFromBody(auth, body, observedAt); errRecord != nil {
			return nil, errRecord
		}
		return nil, nil
	})
	if err != nil {
		log.WithError(err).Debug("quota estimator: failed to auto-refresh wham/usage observation")
	}
}

func (e *Estimator) fetchUsageSnapshot(ctx context.Context, auth *coreauth.Auth) ([]byte, error) {
	e.mu.RLock()
	fetcher := e.usageFetcher
	e.mu.RUnlock()
	if fetcher == nil {
		return nil, errors.New("quota estimator: usage fetcher not configured")
	}
	return fetcher(ctx, auth)
}

func (e *Estimator) hasDueRefresh(authIndex string, recordedAt time.Time) bool {
	if e == nil || authIndex == "" || recordedAt.IsZero() {
		return false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.hasDueRefreshLocked(authIndex, recordedAt)
}

func (e *Estimator) hasDueRefreshLocked(authIndex string, recordedAt time.Time) bool {
	if recordedAt.IsZero() {
		return false
	}
	for _, key := range e.openCycleKeysForAuthLocked(authIndex) {
		nextRefreshAt := e.state.OpenCycles[key].NextRefreshAt
		if !nextRefreshAt.IsZero() && !recordedAt.Before(nextRefreshAt) {
			return true
		}
	}
	return false
}

// RecordExhaustionEvent stores a passive usage_limit_reached event for the selected Codex auth.
func (e *Estimator) RecordExhaustionEvent(auth *coreauth.Auth, model string, retryAfter *time.Duration, observedAt time.Time) {
	if e == nil || !IsCodexOAuthAuth(auth) {
		return
	}
	authIndex := auth.EnsureIndex()
	if authIndex == "" {
		return
	}
	observedAt = normalizeTime(observedAt)
	if observedAt.IsZero() {
		observedAt = normalizeTime(e.now())
	}
	event := ExhaustionEvent{
		ObservedAt: observedAt,
		AuthID:     strings.TrimSpace(auth.ID),
		AuthIndex:  authIndex,
		PlanType:   normalizePlanType(auth.Attributes["plan_type"]),
		Model:      strings.TrimSpace(model),
	}
	if retryAfter != nil && *retryAfter > 0 {
		event.RetryAfterSeconds = int64(math.Ceil(retryAfter.Seconds()))
		event.RetryAfterDeadline = observedAt.Add(*retryAfter)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureStateLocked()

	exhaustions := append(cloneExhaustions(e.state.Exhaustions[authIndex]), event)
	e.state.Exhaustions[authIndex] = keepTail(exhaustions, maxExhaustionEventsPerAuth)

	for _, key := range e.openCycleKeysForAuthLocked(authIndex) {
		cycle := e.state.OpenCycles[key]
		cycle.AuthID = firstNonEmpty(cycle.AuthID, event.AuthID)
		cycle.PlanType = firstNonEmpty(cycle.PlanType, event.PlanType)
		cycle.LastExhaustionAt = observedAt
		cycle.ExhaustedPending = true
		e.state.OpenCycles[key] = cycle
	}
	e.recomputeCurrentEstimatesLocked(authIndex)
	e.schedulePersistLocked()
}

// CurrentEstimates returns a copy of the current estimate windows for one auth.
func (e *Estimator) CurrentEstimates(authIndex string) []CurrentCycleEstimate {
	if e == nil {
		return nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return cloneCurrentEstimates(e.state.CurrentEstimate[strings.TrimSpace(authIndex)])
}

// Summaries returns the current per-auth summaries for all known Codex OAuth accounts.
func (e *Estimator) Summaries(auths []*coreauth.Auth) []CodexAccountSummary {
	if e == nil {
		return nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	authByIndex := make(map[string]*coreauth.Auth)
	for _, auth := range auths {
		if !IsCodexOAuthAuth(auth) {
			continue
		}
		index := auth.EnsureIndex()
		if index == "" {
			continue
		}
		authByIndex[index] = auth.Clone()
	}

	indices := make(map[string]struct{})
	for index := range authByIndex {
		indices[index] = struct{}{}
	}
	for index := range e.state.CurrentEstimate {
		indices[index] = struct{}{}
	}
	for index := range e.state.Observations {
		indices[index] = struct{}{}
	}
	for index := range e.state.Exhaustions {
		indices[index] = struct{}{}
	}

	keys := make([]string, 0, len(indices))
	for index := range indices {
		keys = append(keys, index)
	}
	sort.Strings(keys)

	out := make([]CodexAccountSummary, 0, len(keys))
	for _, index := range keys {
		out = append(out, e.buildSummaryLocked(index, authByIndex[index]))
	}
	return out
}

// Detail returns the detailed estimator view for a single auth index.
func (e *Estimator) Detail(authIndex string, auth *coreauth.Auth) CodexAccountDetail {
	result := CodexAccountDetail{}
	if e == nil {
		return result
	}
	authIndex = strings.TrimSpace(authIndex)
	e.mu.RLock()
	defer e.mu.RUnlock()

	if auth != nil {
		auth = auth.Clone()
	}
	summary := e.buildSummaryLocked(authIndex, auth)
	result.CodexAccountSummary = summary

	if observation, ok := lastObservation(e.state.Observations[authIndex]); ok {
		copyObservation := observation
		copyObservation.Windows = cloneWindows(copyObservation.Windows)
		result.RecentObservation = &copyObservation
	}
	result.Observations = cloneObservations(e.state.Observations[authIndex])
	result.Exhaustions = cloneExhaustions(e.state.Exhaustions[authIndex])
	result.ClosedSamples = e.closedSamplesForAuthLocked(authIndex)
	result.OpenCycles = e.openCyclesForAuthLocked(authIndex)
	result.CurrentEstimates = e.currentEstimateMapForAuthLocked(authIndex)
	return result
}

// ParseObservation converts a raw wham/usage response into the estimator observation shape.
func ParseObservation(raw []byte, auth *coreauth.Auth, observedAt time.Time) (QuotaObservation, error) {
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return QuotaObservation{}, fmt.Errorf("quota estimator: invalid observation payload: %w", err)
	}
	authID := ""
	authIndex := ""
	planType := ""
	if auth != nil {
		authID = strings.TrimSpace(auth.ID)
		authIndex = auth.EnsureIndex()
		planType = normalizePlanType(auth.Attributes["plan_type"])
	}

	if parsedPlan := detectPlanType(payload); parsedPlan != "" {
		planType = parsedPlan
	}
	windows := collectWindows(payload, nil, nil)
	if len(windows) == 0 {
		return QuotaObservation{}, errors.New("quota estimator: no quota windows found in observation")
	}
	sort.Slice(windows, func(i, j int) bool {
		return compareWindowType(windows[i].WindowType, windows[j].WindowType)
	})
	return QuotaObservation{
		ObservedAt: normalizeTime(observedAt),
		AuthID:     authID,
		AuthIndex:  authIndex,
		PlanType:   planType,
		Windows:    dedupeWindows(windows),
		RawSummary: buildRawSummary(planType, windows),
	}, nil
}

// IsCodexOAuthAuth reports whether the auth is a Codex OAuth account instead of a Codex API key entry.
func IsCodexOAuthAuth(auth *coreauth.Auth) bool {
	if auth == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return false
	}
	if auth.Attributes != nil {
		if key := strings.TrimSpace(auth.Attributes["api_key"]); key != "" {
			return false
		}
	}
	kind, _ := auth.AccountInfo()
	return !strings.EqualFold(kind, "api_key")
}

func (e *Estimator) ensureStateLocked() {
	if e.state.Observations == nil {
		e.state = newEstimatorState()
	}
	if e.state.Observations == nil {
		e.state.Observations = make(map[string][]QuotaObservation)
	}
	if e.state.OpenCycles == nil {
		e.state.OpenCycles = make(map[string]OpenQuotaCycle)
	}
	if e.state.ClosedSamples == nil {
		e.state.ClosedSamples = make(map[string][]QuotaCycleSample)
	}
	if e.state.Exhaustions == nil {
		e.state.Exhaustions = make(map[string][]ExhaustionEvent)
	}
	if e.state.PendingUsage == nil {
		e.state.PendingUsage = make(map[string]pendingUsage)
	}
	if e.state.CurrentEstimate == nil {
		e.state.CurrentEstimate = make(map[string][]CurrentCycleEstimate)
	}
}

func (e *Estimator) closeAllOpenCyclesLocked(authIndex string, endedAt time.Time, closeReason string) {
	for _, key := range e.openCycleKeysForAuthLocked(authIndex) {
		cycle := e.state.OpenCycles[key]
		e.closeCycleLocked(key, cycle, endedAt, cycle.CurrentUsedPercent, closeReason)
	}
}

func (e *Estimator) closeCycleLocked(key string, cycle OpenQuotaCycle, endedAt time.Time, endUsedPercent float64, closeReason string) {
	bucket := bucketKey(cycle.PlanType, cycle.WindowType)
	sample := QuotaCycleSample{
		AuthID:           cycle.AuthID,
		AuthIndex:        cycle.AuthIndex,
		CycleStartAt:     normalizeTime(cycle.StartedAt),
		CycleEndAt:       normalizeTime(endedAt),
		PlanType:         normalizePlanType(cycle.PlanType),
		WindowType:       normalizeWindowType(cycle.WindowType),
		StartUsedPercent: roundPercent(cycle.StartUsedPercent),
		EndUsedPercent:   roundPercent(endUsedPercent),
		CloseReason:      strings.TrimSpace(closeReason),
		Tokens:           cycle.Tokens,
		PerModel:         cloneModelTokens(cycle.PerModel),
	}
	samples := append(cloneSamples(e.state.ClosedSamples[bucket]), sample)
	e.state.ClosedSamples[bucket] = keepTail(samples, maxClosedSamplesPerBucket)
	delete(e.state.OpenCycles, key)
}

func (e *Estimator) recomputeCurrentEstimatesLocked(authIndex string) {
	estimates := make([]CurrentCycleEstimate, 0)
	for _, key := range e.openCycleKeysForAuthLocked(authIndex) {
		cycle := e.state.OpenCycles[key]
		capacity, sampleCount := estimateCapacity(e.state.ClosedSamples[bucketKey(cycle.PlanType, cycle.WindowType)])
		estimate := CurrentCycleEstimate{
			AuthID:                cycle.AuthID,
			AuthIndex:             cycle.AuthIndex,
			PlanType:              cycle.PlanType,
			WindowType:            cycle.WindowType,
			CurrentCycleStartedAt: cycle.StartedAt,
			CycleStartSource:      cycle.StartSource,
			LastRefreshAt:         cycle.LastRefreshAt,
			CurrentUsedPercent:    roundPercent(cycle.CurrentUsedPercent),
			CurrentTokens:         cycle.Tokens,
			PerModel:              cloneModelTokens(cycle.PerModel),
			EstimatedCapacity:     capacity,
			SampleCount:           sampleCount,
			Confidence:            confidenceForCycle(cycle.StartSource, sampleCount),
			LastExhaustionAt:      cycle.LastExhaustionAt,
			ExhaustedPending:      cycle.ExhaustedPending,
			ExhaustedConfirmed:    cycle.ExhaustedConfirmed,
		}
		estimates = append(estimates, estimate)
	}
	sort.Slice(estimates, func(i, j int) bool {
		return compareWindowType(estimates[i].WindowType, estimates[j].WindowType)
	})
	if len(estimates) == 0 {
		delete(e.state.CurrentEstimate, authIndex)
		return
	}
	e.state.CurrentEstimate[authIndex] = estimates
}

func (e *Estimator) openCycleKeysForAuthLocked(authIndex string) []string {
	keys := make([]string, 0)
	prefix := authIndex + "|"
	for key := range e.state.OpenCycles {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func (e *Estimator) buildSummaryLocked(authIndex string, auth *coreauth.Auth) CodexAccountSummary {
	summary := CodexAccountSummary{
		AuthIndex: authIndex,
		Windows:   cloneCurrentEstimates(e.state.CurrentEstimate[authIndex]),
	}
	if auth != nil {
		summary.AuthID = strings.TrimSpace(auth.ID)
		summary.AccountType, summary.Account = auth.AccountInfo()
		summary.PlanType = normalizePlanType(auth.Attributes["plan_type"])
	}
	if summary.AuthID == "" {
		if observation, ok := lastObservation(e.state.Observations[authIndex]); ok {
			summary.AuthID = observation.AuthID
		}
	}
	if summary.PlanType == "" {
		summary.PlanType = planTypeFromEstimate(summary.Windows)
	}
	if summary.PlanType == "" {
		if observation, ok := lastObservation(e.state.Observations[authIndex]); ok {
			summary.PlanType = observation.PlanType
		}
	}
	if observation, ok := lastObservation(e.state.Observations[authIndex]); ok {
		summary.LastObservationAt = observation.ObservedAt
	}
	if exhaustions := e.state.Exhaustions[authIndex]; len(exhaustions) > 0 {
		last := exhaustions[len(exhaustions)-1]
		copyLast := last
		summary.LastExhaustion = &copyLast
	}
	for _, estimate := range summary.Windows {
		if estimate.LastRefreshAt.After(summary.LastQuotaRefreshAt) {
			summary.LastQuotaRefreshAt = estimate.LastRefreshAt
		}
	}
	return summary
}

func (e *Estimator) currentEstimateMapForAuthLocked(authIndex string) map[string]CurrentCycleEstimate {
	estimates := cloneCurrentEstimates(e.state.CurrentEstimate[authIndex])
	if len(estimates) == 0 {
		return nil
	}
	out := make(map[string]CurrentCycleEstimate, len(estimates))
	for _, estimate := range estimates {
		out[estimate.WindowType] = estimate
	}
	return out
}

func (e *Estimator) closedSamplesForAuthLocked(authIndex string) map[string][]QuotaCycleSample {
	result := make(map[string][]QuotaCycleSample)
	for _, samples := range e.state.ClosedSamples {
		for _, sample := range samples {
			if sample.AuthIndex != authIndex {
				continue
			}
			windowType := sample.WindowType
			result[windowType] = append(result[windowType], cloneSample(sample))
		}
	}
	for windowType := range result {
		sort.Slice(result[windowType], func(i, j int) bool {
			return result[windowType][i].CycleEndAt.After(result[windowType][j].CycleEndAt)
		})
	}
	return result
}

func (e *Estimator) openCyclesForAuthLocked(authIndex string) map[string]OpenQuotaCycle {
	result := make(map[string]OpenQuotaCycle)
	for _, key := range e.openCycleKeysForAuthLocked(authIndex) {
		cycle := e.state.OpenCycles[key]
		cycle.PerModel = cloneModelTokens(cycle.PerModel)
		result[cycle.WindowType] = cycle
	}
	return result
}

func (e *Estimator) schedulePersistLocked() {
	if e.path == "" || e.closed {
		return
	}
	e.persistDirty = true
	if e.persistTimer == nil {
		e.persistTimer = time.AfterFunc(e.persistDebounce, e.flushPersist)
		return
	}
	e.persistTimer.Reset(e.persistDebounce)
}

func (e *Estimator) flushPersist() {
	e.mu.Lock()
	if !e.persistDirty || e.closed {
		e.mu.Unlock()
		return
	}
	snapshot := e.cloneStateLocked()
	e.persistDirty = false
	e.mu.Unlock()

	if err := e.save(snapshot); err != nil {
		log.WithError(err).Warn("quota estimator: failed to persist state")
	}
}

func (e *Estimator) cloneStateLocked() estimatorState {
	state := newEstimatorState()
	for key, observations := range e.state.Observations {
		state.Observations[key] = cloneObservations(observations)
	}
	for key, cycle := range e.state.OpenCycles {
		cycle.PerModel = cloneModelTokens(cycle.PerModel)
		state.OpenCycles[key] = cycle
	}
	for key, samples := range e.state.ClosedSamples {
		state.ClosedSamples[key] = cloneSamples(samples)
	}
	for key, exhaustions := range e.state.Exhaustions {
		state.Exhaustions[key] = cloneExhaustions(exhaustions)
	}
	for key, pending := range e.state.PendingUsage {
		pending.PerModel = cloneModelTokens(pending.PerModel)
		state.PendingUsage[key] = pending
	}
	for key, estimates := range e.state.CurrentEstimate {
		state.CurrentEstimate[key] = cloneCurrentEstimates(estimates)
	}
	return state
}

func (e *Estimator) load() error {
	if e.path == "" {
		return nil
	}
	raw, err := os.ReadFile(e.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var persisted persistedState
	if errUnmarshal := json.Unmarshal(raw, &persisted); errUnmarshal != nil {
		return errUnmarshal
	}
	if persisted.Version != 0 && persisted.Version != stateVersion {
		return fmt.Errorf("unsupported quota estimator version %d", persisted.Version)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state = persisted.estimatorState
	e.ensureStateLocked()
	return nil
}

func (e *Estimator) save(state estimatorState) error {
	if e.path == "" {
		return nil
	}
	dir := filepath.Dir(e.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	payload, errMarshal := json.MarshalIndent(persistedState{
		Version:        stateVersion,
		estimatorState: state,
	}, "", "  ")
	if errMarshal != nil {
		return errMarshal
	}
	tmpPath := e.path + ".tmp"
	if errWrite := os.WriteFile(tmpPath, payload, 0o600); errWrite != nil {
		return errWrite
	}
	return os.Rename(tmpPath, e.path)
}

func openCycleKey(authIndex, windowType string) string {
	return strings.TrimSpace(authIndex) + "|" + normalizeWindowType(windowType)
}

func bucketKey(planType, windowType string) string {
	return normalizePlanType(planType) + "|" + normalizeWindowType(windowType)
}

func normalizeWindowObservation(window QuotaWindowObservation) QuotaWindowObservation {
	window.WindowType = normalizeWindowType(window.WindowType)
	window.UsedPercent = roundPercent(window.UsedPercent)
	window.ResetIdentifier = strings.TrimSpace(window.ResetIdentifier)
	window.ResetAt = normalizeTime(window.ResetAt)
	return window
}

func normalizePlanType(planType string) string {
	return strings.ToLower(strings.TrimSpace(planType))
}

func normalizeWindowType(windowType string) string {
	windowType = strings.ToLower(strings.TrimSpace(windowType))
	switch {
	case windowType == "":
		return ""
	case strings.Contains(windowType, "5") && (strings.Contains(windowType, "h") || strings.Contains(windowType, "hour")):
		return "5h"
	case strings.Contains(windowType, "7") && (strings.Contains(windowType, "d") || strings.Contains(windowType, "day")):
		return "7d"
	case strings.Contains(windowType, "daily"):
		return "1d"
	case strings.Contains(windowType, "weekly"):
		return "7d"
	default:
		return windowType
	}
}

func normalizeTime(ts time.Time) time.Time {
	if ts.IsZero() {
		return time.Time{}
	}
	return ts.UTC()
}

func roundPercent(v float64) float64 {
	return math.Round(v*100) / 100
}

func shouldRefreshCycle(cycle OpenQuotaCycle, prevWindow QuotaWindowObservation, hasPrevWindow bool, currentWindow QuotaWindowObservation) bool {
	currentUsed := currentWindow.UsedPercent
	prevUsed := cycle.CurrentUsedPercent
	if hasPrevWindow {
		prevUsed = prevWindow.UsedPercent
		if prevWindow.ResetIdentifier != "" && currentWindow.ResetIdentifier != "" && prevWindow.ResetIdentifier != currentWindow.ResetIdentifier {
			return true
		}
	}
	return currentUsed+observationNoisePercent < prevUsed
}

func closeReasonFromTransition(cycle OpenQuotaCycle, prevWindow QuotaWindowObservation, hasPrevWindow bool) string {
	prevUsed := cycle.CurrentUsedPercent
	if hasPrevWindow {
		prevUsed = prevWindow.UsedPercent
	}
	if cycle.ExhaustedPending || cycle.ExhaustedConfirmed {
		return closeReasonExhaustedRefreshed
	}
	if prevUsed >= exhaustedThresholdPercent {
		return closeReasonQuotaRefreshed
	}
	return closeReasonRefreshedBeforeFull
}

func estimateCapacity(samples []QuotaCycleSample) (TokenSummary, int) {
	filtered := make([]TokenSummary, 0, len(samples))
	for _, sample := range samples {
		if sample.CloseReason == closeReasonPlanTypeChanged {
			continue
		}
		deltaPercent := sample.EndUsedPercent - sample.StartUsedPercent
		if deltaPercent <= observationNoisePercent {
			continue
		}
		scale := 100.0 / deltaPercent
		filtered = append(filtered, scaleTokens(sample.Tokens, scale))
	}
	if len(filtered) == 0 {
		return TokenSummary{}, 0
	}
	return TokenSummary{
		ReadTokens:      medianField(filtered, func(s TokenSummary) int64 { return s.ReadTokens }),
		CacheReadTokens: medianField(filtered, func(s TokenSummary) int64 { return s.CacheReadTokens }),
		OutputTokens:    medianField(filtered, func(s TokenSummary) int64 { return s.OutputTokens }),
		ReasoningTokens: medianField(filtered, func(s TokenSummary) int64 { return s.ReasoningTokens }),
		TotalTokens:     medianField(filtered, func(s TokenSummary) int64 { return s.TotalTokens }),
	}, len(filtered)
}

func confidenceForCycle(startSource string, sampleCount int) string {
	if startSource != startSourceQuotaRefresh {
		return "low"
	}
	switch {
	case sampleCount >= 3:
		return "high"
	case sampleCount >= 1:
		return "medium"
	default:
		return "low"
	}
}

func applyPendingUsage(cycle *OpenQuotaCycle, pending pendingUsage) {
	if cycle == nil {
		return
	}
	if cycle.PerModel == nil {
		cycle.PerModel = make(map[string]TokenSummary)
	}
	addTokenSummary(&cycle.Tokens, pending.Tokens)
	for model, summary := range pending.PerModel {
		addModelUsage(cycle.PerModel, model, summary)
	}
}

func addTokenSummary(target *TokenSummary, delta TokenSummary) {
	if target == nil {
		return
	}
	target.ReadTokens += delta.ReadTokens
	target.CacheReadTokens += delta.CacheReadTokens
	target.OutputTokens += delta.OutputTokens
	target.ReasoningTokens += delta.ReasoningTokens
	target.TotalTokens += delta.TotalTokens
}

func addModelUsage(models map[string]TokenSummary, model string, summary TokenSummary) {
	if models == nil {
		return
	}
	current := models[model]
	addTokenSummary(&current, summary)
	models[model] = current
}

func tokenSummaryFromDetail(detail coreusage.Detail) TokenSummary {
	readTokens := detail.InputTokens - detail.CachedTokens
	if readTokens < 0 {
		readTokens = 0
	}
	totalTokens := detail.TotalTokens
	if totalTokens == 0 {
		totalTokens = detail.InputTokens + detail.OutputTokens + detail.ReasoningTokens
	}
	if totalTokens == 0 {
		totalTokens = readTokens + detail.CachedTokens + detail.OutputTokens + detail.ReasoningTokens
	}
	return TokenSummary{
		ReadTokens:      readTokens,
		CacheReadTokens: detail.CachedTokens,
		OutputTokens:    detail.OutputTokens,
		ReasoningTokens: detail.ReasoningTokens,
		TotalTokens:     totalTokens,
	}
}

func scaleTokens(summary TokenSummary, scale float64) TokenSummary {
	return TokenSummary{
		ReadTokens:      int64(math.Round(float64(summary.ReadTokens) * scale)),
		CacheReadTokens: int64(math.Round(float64(summary.CacheReadTokens) * scale)),
		OutputTokens:    int64(math.Round(float64(summary.OutputTokens) * scale)),
		ReasoningTokens: int64(math.Round(float64(summary.ReasoningTokens) * scale)),
		TotalTokens:     int64(math.Round(float64(summary.TotalTokens) * scale)),
	}
}

func medianField(samples []TokenSummary, getter func(TokenSummary) int64) int64 {
	values := make([]int64, 0, len(samples))
	for _, sample := range samples {
		values = append(values, getter(sample))
	}
	slices.Sort(values)
	return values[len(values)/2]
}

func buildRawSummary(planType string, windows []QuotaWindowObservation) string {
	type windowSummary struct {
		WindowType      string  `json:"window_type"`
		UsedPercent     float64 `json:"used_percent"`
		ResetIdentifier string  `json:"reset_identifier,omitempty"`
	}
	summary := struct {
		PlanType string          `json:"plan_type"`
		Windows  []windowSummary `json:"windows"`
	}{
		PlanType: planType,
		Windows:  make([]windowSummary, 0, len(windows)),
	}
	for _, window := range windows {
		summary.Windows = append(summary.Windows, windowSummary{
			WindowType:      window.WindowType,
			UsedPercent:     roundPercent(window.UsedPercent),
			ResetIdentifier: window.ResetIdentifier,
		})
	}
	payload, errMarshal := json.Marshal(summary)
	if errMarshal != nil {
		return ""
	}
	return string(payload)
}

func collectWindows(node any, path []string, windows []QuotaWindowObservation) []QuotaWindowObservation {
	switch typed := node.(type) {
	case map[string]any:
		if window, ok := parseWindowObservation(typed, path); ok {
			windows = append(windows, window)
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			windows = collectWindows(typed[key], append(path, key), windows)
		}
	case []any:
		for idx, item := range typed {
			windows = collectWindows(item, append(path, strconv.Itoa(idx)), windows)
		}
	}
	return windows
}

func parseWindowObservation(node map[string]any, path []string) (QuotaWindowObservation, bool) {
	usedPercent, ok := extractUsedPercent(node)
	if !ok {
		return QuotaWindowObservation{}, false
	}
	windowType := detectWindowType(node, path)
	if windowType == "" {
		return QuotaWindowObservation{}, false
	}
	window := QuotaWindowObservation{
		WindowType:      windowType,
		UsedPercent:     roundPercent(usedPercent),
		Available:       extractAvailability(node),
		ResetAt:         extractTime(node, "reset_at", "resetAt", "resets_at", "resetsAt", "next_reset_at", "nextResetAt", "period_start", "periodStart"),
		ResetIdentifier: extractResetIdentifier(node),
	}
	if window.ResetIdentifier == "" && !window.ResetAt.IsZero() {
		window.ResetIdentifier = window.ResetAt.Format(time.RFC3339)
	}
	return window, true
}

func extractUsedPercent(node map[string]any) (float64, bool) {
	for _, key := range []string{"used_percent", "usedPercent", "usage_percent", "usagePercent", "percent_used", "percentUsed"} {
		if value, ok := floatValue(node[key]); ok {
			return clampPercent(value), true
		}
	}
	for _, key := range []string{"used_ratio", "usedRatio", "used_fraction", "usedFraction"} {
		if value, ok := floatValue(node[key]); ok {
			return clampPercent(value * 100), true
		}
	}
	for _, key := range []string{"remaining_percent", "remainingPercent"} {
		if value, ok := floatValue(node[key]); ok {
			return clampPercent(100 - value), true
		}
	}
	for _, key := range []string{"remaining_ratio", "remainingRatio", "remaining_fraction", "remainingFraction"} {
		if value, ok := floatValue(node[key]); ok {
			return clampPercent(100 - value*100), true
		}
	}

	usedKeys := []string{"used", "usage", "consumed", "current_usage", "currentUsage"}
	limitKeys := []string{"limit", "max", "quota", "allowance", "capacity", "total"}
	var usedValue, limitValue float64
	var usedOK, limitOK bool
	for _, key := range usedKeys {
		if usedValue, usedOK = floatValue(node[key]); usedOK {
			break
		}
	}
	for _, key := range limitKeys {
		if limitValue, limitOK = floatValue(node[key]); limitOK {
			break
		}
	}
	if usedOK && limitOK && limitValue > 0 {
		return clampPercent(usedValue / limitValue * 100), true
	}
	return 0, false
}

func extractAvailability(node map[string]any) bool {
	for _, key := range []string{"available", "is_available", "isAvailable", "enabled", "has_access", "hasAccess"} {
		if value, ok := boolValue(node[key]); ok {
			return value
		}
	}
	return true
}

func detectWindowType(node map[string]any, path []string) string {
	for _, key := range []string{"quota_window_type", "quotaWindowType", "window_type", "windowType", "period", "bucket", "name", "slug", "id"} {
		if raw, ok := stringValue(node[key]); ok {
			if detected := normalizeWindowType(raw); detected != "" {
				return detected
			}
		}
	}
	for i := len(path) - 1; i >= 0; i-- {
		if detected := normalizePathWindowType(path[i]); detected != "" {
			return detected
		}
	}
	return ""
}

func normalizePathWindowType(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch {
	case value == "":
		return ""
	case strings.Contains(value, "primary_window"), strings.Contains(value, "primarywindow"), value == "primary":
		return "5h"
	case strings.Contains(value, "secondary_window"), strings.Contains(value, "secondarywindow"), value == "secondary":
		return "7d"
	case strings.Contains(value, "five_hour"), strings.Contains(value, "5h"), strings.Contains(value, "hour_5"), strings.Contains(value, "5_hour"):
		return "5h"
	case strings.Contains(value, "seven_day"), strings.Contains(value, "7d"), strings.Contains(value, "day_7"), strings.Contains(value, "7_day"):
		return "7d"
	case strings.Contains(value, "daily"):
		return "1d"
	case strings.Contains(value, "weekly"):
		return "7d"
	default:
		return ""
	}
}

func detectPlanType(node any) string {
	switch typed := node.(type) {
	case map[string]any:
		for _, key := range []string{"plan_type", "planType"} {
			if raw, ok := stringValue(typed[key]); ok {
				if planType := normalizePlanType(raw); planType != "" {
					return planType
				}
			}
		}
		for _, key := range []string{"plan", "subscription", "account"} {
			if nested, ok := typed[key]; ok {
				if planType := detectPlanType(nested); planType != "" {
					return planType
				}
			}
		}
		for _, value := range typed {
			if planType := detectPlanType(value); planType != "" {
				return planType
			}
		}
	case []any:
		for _, value := range typed {
			if planType := detectPlanType(value); planType != "" {
				return planType
			}
		}
	}
	return ""
}

func extractResetIdentifier(node map[string]any) string {
	for _, key := range []string{"reset_identifier", "resetIdentifier", "window_id", "windowId", "period_id", "periodId", "period_start", "periodStart"} {
		if raw, ok := stringValue(node[key]); ok {
			return strings.TrimSpace(raw)
		}
		if ts, ok := timeValue(node[key]); ok {
			return ts.UTC().Format(time.RFC3339)
		}
	}
	return ""
}

func extractTime(node map[string]any, keys ...string) time.Time {
	for _, key := range keys {
		if ts, ok := timeValue(node[key]); ok {
			return ts.UTC()
		}
	}
	return time.Time{}
}

func dedupeWindows(windows []QuotaWindowObservation) []QuotaWindowObservation {
	seen := make(map[string]QuotaWindowObservation)
	for _, window := range windows {
		window = normalizeWindowObservation(window)
		if window.WindowType == "" {
			continue
		}
		previous, exists := seen[window.WindowType]
		if !exists || (previous.ResetIdentifier == "" && window.ResetIdentifier != "") {
			seen[window.WindowType] = window
		}
	}
	out := make([]QuotaWindowObservation, 0, len(seen))
	for _, window := range seen {
		out = append(out, window)
	}
	sort.Slice(out, func(i, j int) bool {
		return compareWindowType(out[i].WindowType, out[j].WindowType)
	})
	return out
}

func compareWindowType(left, right string) bool {
	order := map[string]int{
		"5h": 0,
		"1d": 1,
		"7d": 2,
	}
	leftOrder, leftOK := order[left]
	rightOrder, rightOK := order[right]
	switch {
	case leftOK && rightOK:
		return leftOrder < rightOrder
	case leftOK:
		return true
	case rightOK:
		return false
	default:
		return left < right
	}
}

func lastObservation(observations []QuotaObservation) (QuotaObservation, bool) {
	if len(observations) == 0 {
		return QuotaObservation{}, false
	}
	observation := observations[len(observations)-1]
	observation.Windows = cloneWindows(observation.Windows)
	return observation, true
}

func planTypeFromEstimate(estimates []CurrentCycleEstimate) string {
	for _, estimate := range estimates {
		if estimate.PlanType != "" {
			return estimate.PlanType
		}
	}
	return ""
}

func keepTail[T any](items []T, max int) []T {
	if max <= 0 || len(items) <= max {
		return items
	}
	return append([]T(nil), items[len(items)-max:]...)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func clampPercent(value float64) float64 {
	switch {
	case math.IsNaN(value), math.IsInf(value, 0):
		return 0
	case value < 0:
		return 0
	case value > 100:
		return 100
	default:
		return value
	}
}

func floatValue(raw any) (float64, bool) {
	switch value := raw.(type) {
	case float64:
		return value, true
	case float32:
		return float64(value), true
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	case int32:
		return float64(value), true
	case json.Number:
		if parsed, err := value.Float64(); err == nil {
			return parsed, true
		}
	case string:
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return 0, false
		}
		if parsed, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func boolValue(raw any) (bool, bool) {
	switch value := raw.(type) {
	case bool:
		return value, true
	case string:
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return false, false
		}
		parsed, err := strconv.ParseBool(trimmed)
		if err != nil {
			return false, false
		}
		return parsed, true
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return false, false
		}
		return parsed != 0, true
	case float64:
		return value != 0, true
	default:
		return false, false
	}
}

func stringValue(raw any) (string, bool) {
	switch value := raw.(type) {
	case string:
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return "", false
		}
		return trimmed, true
	case json.Number:
		return value.String(), true
	default:
		return "", false
	}
}

func timeValue(raw any) (time.Time, bool) {
	switch value := raw.(type) {
	case time.Time:
		if value.IsZero() {
			return time.Time{}, false
		}
		return value.UTC(), true
	case json.Number:
		if intValue, err := value.Int64(); err == nil {
			return unixTime(intValue), true
		}
	case float64:
		return unixTime(int64(value)), true
	case int64:
		return unixTime(value), true
	case int:
		return unixTime(int64(value)), true
	case string:
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return time.Time{}, false
		}
		if unixSeconds, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return unixTime(unixSeconds), true
		}
		for _, layout := range []string{time.RFC3339, time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02 15:04"} {
			if parsed, err := time.Parse(layout, trimmed); err == nil {
				return parsed.UTC(), true
			}
		}
	}
	return time.Time{}, false
}

func unixTime(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	if value > 1_000_000_000_000 {
		return time.UnixMilli(value).UTC()
	}
	return time.Unix(value, 0).UTC()
}

func cloneWindows(windows []QuotaWindowObservation) []QuotaWindowObservation {
	if len(windows) == 0 {
		return nil
	}
	out := make([]QuotaWindowObservation, len(windows))
	copy(out, windows)
	return out
}

func cloneObservations(observations []QuotaObservation) []QuotaObservation {
	if len(observations) == 0 {
		return nil
	}
	out := make([]QuotaObservation, len(observations))
	for i, observation := range observations {
		observation.Windows = cloneWindows(observation.Windows)
		out[i] = observation
	}
	return out
}

func cloneSamples(samples []QuotaCycleSample) []QuotaCycleSample {
	if len(samples) == 0 {
		return nil
	}
	out := make([]QuotaCycleSample, len(samples))
	for i, sample := range samples {
		out[i] = cloneSample(sample)
	}
	return out
}

func cloneSample(sample QuotaCycleSample) QuotaCycleSample {
	sample.PerModel = cloneModelTokens(sample.PerModel)
	return sample
}

func cloneExhaustions(events []ExhaustionEvent) []ExhaustionEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]ExhaustionEvent, len(events))
	copy(out, events)
	return out
}

func cloneCurrentEstimates(estimates []CurrentCycleEstimate) []CurrentCycleEstimate {
	if len(estimates) == 0 {
		return nil
	}
	out := make([]CurrentCycleEstimate, len(estimates))
	for i, estimate := range estimates {
		estimate.PerModel = cloneModelTokens(estimate.PerModel)
		out[i] = estimate
	}
	return out
}

func cloneModelTokens(input map[string]TokenSummary) map[string]TokenSummary {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]TokenSummary, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
