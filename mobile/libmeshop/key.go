package libmeshop

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ElshadHu/meshop/pkg/mesh"
)

const (
	keyFileMode    = 0o600
	keyFileDirMode = 0o700
)

// LoadOrCreateKey reads a 64-byte static key from path
func LoadOrCreateKey(path string) (mesh.StaticKey, bool, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		var k mesh.StaticKey
		if err := k.UnmarshalBinary(b); err != nil {
			return mesh.StaticKey{}, false, fmt.Errorf("decode %s: %w", path, err)
		}
		return k, false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return mesh.StaticKey{}, false, fmt.Errorf("read %s: %w", path, err)
	}
	if err := os.MkdirAll(filepath.Dir(path), keyFileDirMode); err != nil {
		return mesh.StaticKey{}, false, fmt.Errorf("mkdir: %w", err)
	}
	k, err := mesh.GenerateKey()
	if err != nil {
		return mesh.StaticKey{}, false, fmt.Errorf("generate: %w", err)
	}
	out, err := k.MarshalBinary()
	if err != nil {
		return mesh.StaticKey{}, false, fmt.Errorf("marshal: %w", err)
	}
	if err := os.WriteFile(path, out, keyFileMode); err != nil {
		return mesh.StaticKey{}, false, fmt.Errorf("write %s: %w", path, err)
	}
	return k, true, nil
}
