package workflow

import (
	"encoding/json"
	"testing"
)

func TestExpandCommand(t *testing.T) {
	got, err := ExpandCommand("charge {{amount}} for user={{user_id}} vip={{vip}}",
		json.RawMessage(`{"user_id":"abc-123","amount":12345678901234567890,"vip":true}`))
	if err != nil {
		t.Fatal(err)
	}
	want := "charge 12345678901234567890 for user=abc-123 vip=true"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExpandCommandRejectsUnsafeInput(t *testing.T) {
	for _, input := range []string{
		`{"user_id":"1; rm -rf /"}`,
		`{"user_id":"1 && whoami"}`,
		`{"user_id":"$(whoami)"}`,
		"{\"user_id\":\"`whoami`\"}",
		`{"user_id":"a|b"}`,
		`{"user_id":"a b"}`,
		`{"user_id":"'quoted'"}`,
		`{"user_id":"--flag"}`,
		`{"user_id":{"nested":1}}`,
		`{"user_id":[1,2]}`,
		`not json`,
		`[1,2]`,
	} {
		if got, err := ExpandCommand("echo {{user_id}}", json.RawMessage(input)); err == nil {
			t.Errorf("input %s: expected an error, got command %q", input, got)
		}
	}
}

func TestParseInputAllowsEmpty(t *testing.T) {
	for _, input := range []string{`{}`, `null`, `{"note":""}`} {
		if _, err := ParseInput(json.RawMessage(input)); err != nil {
			t.Errorf("input %s: unexpected error: %v", input, err)
		}
	}
}
