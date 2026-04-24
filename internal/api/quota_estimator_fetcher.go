package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	executorhelps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	codexWhamUsageURL       = "https://chatgpt.com/backend-api/wham/usage"
	codexWhamUsageUserAgent = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
)

func (s *Server) fetchQuotaEstimatorUsage(ctx context.Context, auth *coreauth.Auth) ([]byte, error) {
	if s == nil || auth == nil {
		return nil, fmt.Errorf("quota estimator: missing server or auth")
	}
	token := quotaEstimatorAccessToken(auth)
	if token == "" {
		return nil, fmt.Errorf("quota estimator: missing codex access token")
	}
	accountID := quotaEstimatorAccountID(auth)
	if accountID == "" {
		return nil, fmt.Errorf("quota estimator: missing chatgpt account id")
	}

	httpClient := executorhelps.NewProxyAwareHTTPClient(ctx, s.cfg, auth, 0)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, codexWhamUsageURL, nil)
	if err != nil {
		return nil, fmt.Errorf("quota estimator: build wham/usage request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", codexWhamUsageUserAgent)
	req.Header.Set("Chatgpt-Account-Id", accountID)

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("quota estimator: execute wham/usage request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("quota estimator: read wham/usage response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("quota estimator: unexpected wham/usage status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func quotaEstimatorAccessToken(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Attributes != nil {
		if token := strings.TrimSpace(auth.Attributes["api_key"]); token != "" {
			return token
		}
	}
	metadata := auth.Metadata
	if len(metadata) == 0 {
		return ""
	}
	for _, key := range []string{"accessToken", "access_token", "token", "id_token"} {
		switch typed := metadata[key].(type) {
		case string:
			if token := strings.TrimSpace(typed); token != "" {
				return token
			}
		case map[string]any:
			if token, ok := typed["access_token"].(string); ok && strings.TrimSpace(token) != "" {
				return strings.TrimSpace(token)
			}
			if token, ok := typed["accessToken"].(string); ok && strings.TrimSpace(token) != "" {
				return strings.TrimSpace(token)
			}
		case map[string]string:
			if token := strings.TrimSpace(typed["access_token"]); token != "" {
				return token
			}
			if token := strings.TrimSpace(typed["accessToken"]); token != "" {
				return token
			}
		}
	}
	return ""
}

func quotaEstimatorAccountID(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Metadata != nil {
		if value, ok := auth.Metadata["account_id"].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	if auth.Attributes != nil {
		for _, key := range []string{"account_id", "chatgpt_account_id"} {
			if value := strings.TrimSpace(auth.Attributes[key]); value != "" {
				return value
			}
		}
	}
	return ""
}
