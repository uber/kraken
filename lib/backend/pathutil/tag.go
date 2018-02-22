package pathutil

import (
	"errors"
	"strings"
)

// ParseRepoTag parses give name to repo and tag. Name should be in the format "repo:tag".
func ParseRepoTag(name string) (string, string, error) {
	tokens := strings.Split(name, ":")
	if len(tokens) != 2 {
		return "", "", errors.New("name must be in format 'repo:tag'")
	}
	repo := tokens[0]
	if len(repo) == 0 {
		return "", "", errors.New("repo must be non-empty")
	}
	tag := tokens[1]
	if len(tag) == 0 {
		return "", "", errors.New("tag must be non-empty")
	}
	return repo, tag, nil
}
