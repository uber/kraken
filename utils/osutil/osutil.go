package osutil

import (
	"bufio"
	"io"
	"os"
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
func ReadLines(filename string) ([]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	var lines []string
	s := bufio.NewScanner(f)
	s.Split(bufio.ScanLines)
	for s.Scan() {
		l := s.Text()
		lines = append(lines, l)
	}
	return lines, nil
}
