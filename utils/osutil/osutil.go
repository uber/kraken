package osutil

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
)

// IsEmpty returns true if directory dir is empty.
func IsEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

// ReadLines returns a list of lines in filename.
func ReadLines(f *os.File) ([]string, error) {
	var lines []string
	s := bufio.NewScanner(f)
	s.Split(bufio.ScanLines)
	for s.Scan() {
		l := s.Text()
		lines = append(lines, l)
	}
	return lines, nil
}

// EnsureFilePresent initializes a file and all parent directories for filepath
// if they do not exist. If the file exists, no-ops.
func EnsureFilePresent(filepath string) error {
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		err := os.MkdirAll(path.Dir(filepath), 0755)
		if err != nil {
			return fmt.Errorf("mkdir: %s", err)
		}
		f, err := os.Create(filepath)
		if err != nil {
			return fmt.Errorf("create: %s", err)
		}
		f.Close()
	} else if err != nil {
		return fmt.Errorf("stat: %s", err)
	}
	return nil
}
