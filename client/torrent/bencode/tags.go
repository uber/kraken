package bencode

import (
	"strings"
)

type tagOptions string

func parseTag(tag string) (string, tagOptions) {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx], tagOptions(tag[idx+1:])
	}
	return tag, tagOptions("")
}

func (opts tagOptions) contains(optionName string) bool {
	if len(opts) == 0 {
		return false
	}

	s := string(opts)
	for s != "" {
		var next string
		i := strings.Index(s, ",")
		if i != -1 {
			s, next = s[:i], s[i+1:]
		}
		if s == optionName {
			return true
		}
		s = next
	}
	return false
}
