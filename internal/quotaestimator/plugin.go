package quotaestimator

import (
	"context"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type usagePlugin struct{}

func init() {
	coreusage.RegisterPlugin(usagePlugin{})
}

func (usagePlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	estimator := FromContext(ctx)
	if estimator == nil {
		return
	}
	estimator.RecordUsageWithContext(ctx, record, SelectedAuthFromContext(ctx))
}
