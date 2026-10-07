package db

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// StringMap is a map[string]string stored as a JSONB object.
type StringMap map[string]string

func (m StringMap) Value() (driver.Value, error) {
	if m == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(m)
}

func (m *StringMap) Scan(src any) error {
	var b []byte
	switch v := src.(type) {
	case nil:
		*m = nil
		return nil
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into StringMap", src)
	}
	return json.Unmarshal(b, m)
}

// nullJSON scans a nullable JSONB column into a json.RawMessage.
type nullJSON struct{ json.RawMessage }

func (n *nullJSON) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		n.RawMessage = nil
	case []byte:
		n.RawMessage = append(json.RawMessage(nil), v...)
	case string:
		n.RawMessage = json.RawMessage(v)
	default:
		return fmt.Errorf("cannot scan %T into JSON", src)
	}
	return nil
}

// jsonValue passes nil for an empty RawMessage, so it is stored as NULL.
func jsonValue(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return []byte(raw)
}
