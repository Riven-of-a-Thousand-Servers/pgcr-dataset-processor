package dirscan

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type FileFinder struct {
	Root string
}

func (ff *FileFinder) FindByExtension(extension string) map[string][]string {
	paths := make(map[string][]string)
	filepath.WalkDir(ff.Root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return filepath.SkipDir
			}
			return err
		}

		if d.IsDir() && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}

		if !d.IsDir() && filepath.Ext(d.Name()) == extension {
			dir := filepath.Dir(path)
			paths[dir] = append(paths[dir], path)
		}
		return nil
	})
	return paths
}
