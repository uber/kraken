package dockerregistry

import "path"

func getNamespace(repo string) string {
	return path.Join("docker", repo)
}
