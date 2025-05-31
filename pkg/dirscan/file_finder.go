package dirscan

import (
	"io/fs"
	"os"
	"path/filepath"
	"pgcr-dataset-processor/pkg/utils"
	"strings"
)

type FileFinder struct {
	Root string
}

func (ff *FileFinder) FindByExtension(extension string) utils.StatefulMap {
	files := make(map[string]*utils.FileStatus)
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
			files[d.Name()] = &utils.FileStatus{
				Path:    path,
				Started: false,
				Done:    false,
			}
		}
		return nil
	})
	return utils.StatefulMap{
		Data:      files,
		Started:   make(chan string, 1),
		Completed: make(chan string, 1),
	}
}
