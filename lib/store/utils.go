package store

import (
	"os"
)

func initDirectories(config *Config) error {
	// Recreate upload, download and trash dirs.
	for _, dir := range []string{config.UploadDir, config.DownloadDir, config.TrashDir} {
		os.RemoveAll(dir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	// We do not want to remove all existing files in cache directory during restart.
	err := os.MkdirAll(config.CacheDir, 0755)
	if err != nil {
		return err
	}

	return nil
}

func createOrUpdateSymlink(sourcePath, targetPath string) error {
	if _, err := os.Stat(targetPath); err == nil {
		if existingSource, err := os.Readlink(targetPath); err != nil {
			return err
		} else if existingSource != sourcePath {
			// If the symlink already exists and points to another valid location, recreate the symlink.
			if err := os.Remove(targetPath); err != nil {
				return err
			}
			if err := os.Symlink(sourcePath, targetPath); err != nil {
				return err
			}
		}
	} else if os.IsNotExist(err) {
		if err := os.Symlink(sourcePath, targetPath); err != nil {
			return err
		}
	} else {
		return err
	}

	return nil
}
