// Package ai is the operations assistant: it classifies and explains failed
// tasks, drafts workflows from plain English, and answers questions about the
// cluster. Everything it sends to a model is redacted first, and it only runs
// for namespaces that opted in (ai_assist).
package ai

import (
	"regexp"
	"strings"
)

// Failure classes.
const (
	// ClassTransient: the same task would likely succeed if retried.
	ClassTransient = "transient"
	// ClassPermanent: retrying will not help; the task or its input is wrong.
	ClassPermanent = "permanent"
	// ClassNeedsAttention: the environment needs a person (credentials,
	// quota, a missing service) before a retry can work.
	ClassNeedsAttention = "needs_attention"
	ClassUnknown        = "unknown"
)

// Classification says what kind of failure this is and how sure we are.
type Classification struct {
	Class      string  `json:"class"`
	Confidence float64 `json:"confidence"`
	Reason     string  `json:"reason,omitempty"`
	Source     string  `json:"source"` // rules or model
}

var rules = []struct {
	re         *regexp.Regexp
	class      string
	confidence float64
	reason     string
}{
	// Needs a person: checked before the transient rules, since a 401 in the
	// middle of a timeout message is still a credentials problem.
	{regexp.MustCompile(`(?i)\b(401|403)\b|unauthori[sz]ed|forbidden|permission denied|access denied|invalid (api[ _-]?key|credentials|token)|authentication failed`),
		ClassNeedsAttention, 0.9, "credentials or permissions are wrong"},
	{regexp.MustCompile(`(?i)insufficient[_ ]quota|quota exceeded|billing|payment required|\b402\b|disk full|no space left on device`),
		ClassNeedsAttention, 0.9, "a quota, bill or disk limit is reached"},

	// Permanent: the task itself is at fault.
	{regexp.MustCompile(`(?i)command not found|executable file not found|no such file or directory|exit status 127|\bexit code 127\b`),
		ClassPermanent, 0.9, "the command or a file it needs does not exist"},
	{regexp.MustCompile(`(?i)syntax error|unexpected token|invalid (json|yaml|argument|input)|cannot parse|failed to parse|undefined variable|unbound variable`),
		ClassPermanent, 0.85, "the command or its input is malformed"},
	{regexp.MustCompile(`(?i)\b(400|404|405|409|410|422)\b.*(bad request|not found|unprocessable|conflict|gone)|bad request|unprocessable entity|unknown model|model .* does not exist|invalid_request_error`),
		ClassPermanent, 0.8, "the request was rejected as invalid"},
	{regexp.MustCompile(`(?i)assertion (failed|error)|\bpanic:|segmentation fault|nullpointerexception`),
		ClassPermanent, 0.7, "the program crashed on its own logic"},

	// Transient: the world was briefly unavailable.
	{regexp.MustCompile(`(?i)connection (refused|reset|timed out|closed)|i/o timeout|timed? ?out|deadline exceeded|temporary failure|try again|network is unreachable|no route to host|broken pipe|\beof\b|tls handshake`),
		ClassTransient, 0.85, "a network call failed or timed out"},
	{regexp.MustCompile(`(?i)\b(429|502|503|504)\b|too many requests|rate limit|service unavailable|bad gateway|gateway time-?out|overloaded|throttl`),
		ClassTransient, 0.9, "the service was busy or rate limiting"},
	{regexp.MustCompile(`(?i)deadlock detected|could not serialize access|too many connections|lock timeout`),
		ClassTransient, 0.85, "a database contention error"},
	{regexp.MustCompile(`(?i)worker (lost|died|crashed)|task timed out|killed|out of memory|oom`),
		ClassTransient, 0.6, "the worker or process was lost"},
}

// ClassifyRules classifies a failure by pattern. ok is false when no rule
// matches; the model is then asked.
func ClassifyRules(errorMessage, output string) (Classification, bool) {
	text := errorMessage + "\n" + tail(output, 2000)
	for _, r := range rules {
		if r.re.MatchString(text) {
			return Classification{Class: r.class, Confidence: r.confidence, Reason: r.reason, Source: "rules"}, true
		}
	}
	return Classification{}, false
}

// validClass reports whether a model's answer is one of the known classes.
func validClass(c string) bool {
	switch c {
	case ClassTransient, ClassPermanent, ClassNeedsAttention, ClassUnknown:
		return true
	}
	return false
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "…" + strings.ToValidUTF8(s[len(s)-n:], "")
}

func head(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}
