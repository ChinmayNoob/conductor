package llm

import "regexp"

// redactions remove secrets from text before it is sent to a model: task
// output and errors can contain keys, tokens and passwords.
var redactions = []struct {
	re   *regexp.Regexp
	with string
}{
	{regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----`), "[REDACTED PRIVATE KEY]"},
	{regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`), "$1 [REDACTED]"},
	{regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}`), "[REDACTED JWT]"},
	{regexp.MustCompile(`\b(sk|pk|rk)-[A-Za-z0-9_-]{16,}`), "[REDACTED KEY]"},
	{regexp.MustCompile(`\b(ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{16,}`), "[REDACTED TOKEN]"},
	{regexp.MustCompile(`\bxox[abprs]-[A-Za-z0-9-]{10,}`), "[REDACTED TOKEN]"},
	{regexp.MustCompile(`\b(AKIA|ASIA)[A-Z0-9]{16}\b`), "[REDACTED AWS KEY]"},
	{regexp.MustCompile(`\bcnd_[a-f0-9]{32,}`), "[REDACTED CONDUCTOR KEY]"},
	// user:password@host in URLs and connection strings
	{regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@]+):[^\s@/]+@`), "$1:[REDACTED]@"},
	// key=value, key: value and "key": "value" for secret-sounding keys
	{regexp.MustCompile(`(?i)\b([A-Za-z0-9_-]*(password|passwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential)s?)(["']?\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,;&]+)`), "$1$3[REDACTED]"},
}

// Redact removes likely secrets from s.
func Redact(s string) string {
	for _, r := range redactions {
		s = r.re.ReplaceAllString(s, r.with)
	}
	return s
}
