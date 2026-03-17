package meta

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type File struct {
	CreatedAt time.Time       `json:"created_at"`
	Kind      string          `json:"kind"`
	Params    json.RawMessage `json:"params,omitempty"`
	Source    any             `json:"source,omitempty"`
	Files     map[string]any  `json:"files,omitempty"`
}

func Write(path string, m File) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
