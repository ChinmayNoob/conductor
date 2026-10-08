package llm

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Price is what a model costs, in US dollars per million tokens.
type Price struct {
	Input  float64 `json:"input"`
	Output float64 `json:"output"`
}

// Prices maps model names to prices. Conductor ships none: prices change
// and differ between providers and contracts, and a wrong built-in price
// would make cost budgets lie. Tokens are always counted; costs only for
// models priced in CONDUCTOR_LLM_PRICES, e.g.
//
//	CONDUCTOR_LLM_PRICES='{"gpt-4o-mini": {"input": 0.15, "output": 0.60}}'
type Prices map[string]Price

// PricesFromEnv parses CONDUCTOR_LLM_PRICES.
func PricesFromEnv() (Prices, error) {
	raw := os.Getenv("CONDUCTOR_LLM_PRICES")
	if raw == "" {
		return Prices{}, nil
	}
	var p Prices
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil, fmt.Errorf("invalid CONDUCTOR_LLM_PRICES: %w", err)
	}
	return p, nil
}

// Cost returns the cost of usage on model, and whether the model is priced.
// A dated variant ("gpt-4o-mini-2024-07-18") uses its base model's price.
func (p Prices) Cost(model string, u Usage) (float64, bool) {
	price, ok := p[model]
	if !ok {
		// The longest priced name that prefixes the model: "gpt-4o-mini"
		// beats "gpt-4o" for "gpt-4o-mini-2024-07-18".
		best := ""
		for name, pr := range p {
			if strings.HasPrefix(model, name+"-") && len(name) > len(best) {
				best, price, ok = name, pr, true
			}
		}
	}
	if !ok {
		return 0, false
	}
	return (float64(u.InputTokens)*price.Input + float64(u.OutputTokens)*price.Output) / 1e6, true
}
