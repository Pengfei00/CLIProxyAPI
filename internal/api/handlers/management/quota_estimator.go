package management

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

// GetCodexQuotaEstimator returns the current quota-cycle summaries for all Codex OAuth auths.
func (h *Handler) GetCodexQuotaEstimator(c *gin.Context) {
	if h == nil || h.quotaEstimator == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "quota estimator unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"accounts": h.quotaEstimator.Summaries(h.listAuths()),
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
	c.JSON(http.StatusOK, h.quotaEstimator.Detail(authIndex, h.authByIndex(authIndex)))
}

func (h *Handler) listAuths() []*coreauth.Auth {
	if h == nil || h.authManager == nil {
		return nil
	}
	return h.authManager.List()
}
