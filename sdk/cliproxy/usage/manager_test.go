package usage

import "testing"

func TestNormalizeDetailDoesNotDoubleCountReasoningTokens(t *testing.T) {
	t.Parallel()

	detail := NormalizeDetail(Detail{
		InputTokens:     10,
		OutputTokens:    20,
		ReasoningTokens: 9,
	})

	if detail.TotalTokens != 30 {
		t.Fatalf("total_tokens = %d, want 30", detail.TotalTokens)
	}
}

func TestNormalizeDetailPreservesExplicitTotalTokens(t *testing.T) {
	t.Parallel()

	detail := NormalizeDetail(Detail{
		InputTokens:     10,
		OutputTokens:    20,
		ReasoningTokens: 9,
		TotalTokens:     41,
	})

	if detail.TotalTokens != 41 {
		t.Fatalf("total_tokens = %d, want 41", detail.TotalTokens)
	}
}
