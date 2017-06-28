package base

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
)

// CompareAndWriteFile updates file with given bytes and returns true only if the file is updated
// correctly; It returns false if error happened or file already contains desired content.
func CompareAndWriteFile(filePath string, b []byte) (bool, error) {
	// Check existence.
	fs, err := os.Stat(filePath)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}

	if os.IsNotExist(err) {
		err = os.MkdirAll(path.Dir(filePath), 0755)
		if err != nil {
			return false, err
		}

		err = ioutil.WriteFile(filePath, b, 0755)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	f, err := os.OpenFile(filePath, os.O_RDWR, 0755)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Compare with existing data, overwrite if different.
	buf := make([]byte, int(fs.Size()))
	_, err = f.Read(buf)
	if err != nil {
		return false, err
	}
	if bytes.Compare(buf, b) == 0 {
		return false, nil
	}

	if len(buf) != len(b) {
		err = f.Truncate(int64(len(b)))
		if err != nil {
			return false, err
		}
	}

	_, err = f.WriteAt(b, 0)
	if err != nil {
		return false, err
	}
	return true, nil
}
