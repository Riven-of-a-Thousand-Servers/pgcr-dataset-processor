package parser

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type FileFinder struct {
	Root string
}

func (ff *FileFinder) FindByExtension(extension string) StatefulMap {
	files := make(map[string]*FileStatus)
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
			files[d.Name()] = &FileStatus{
				Path:    path,
				Started: false,
				Done:    false,
			}
		}
		return nil
	})
	return StatefulMap{
		Data:      files,
		Started:   make(chan string, 1),
		Completed: make(chan string, 1),
	}
}
