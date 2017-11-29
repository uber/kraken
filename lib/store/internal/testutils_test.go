package internal

import (
	"regexp"
	"strings"
)

// Mock metadata
func init() {
	RegisterMetadata(regexp.MustCompile("_mocksuffix_\\w+"), &mockMetadataFactory{})
	RegisterMetadata(regexp.MustCompile("_mocksuffix_movable"), &mockMetadataFactoryMovable{})
}

type mockMetadataFactory struct{}

func (f mockMetadataFactory) Create(suffix string) MetadataType {
	if strings.HasSuffix(suffix, getMockMetadataOne().GetSuffix()) {
		return getMockMetadataOne()
	}
	if strings.HasSuffix(suffix, getMockMetadataTwo().GetSuffix()) {
		return getMockMetadataTwo()
	}
	return nil
}

type mockMetadata struct {
	randomSuffix string
}

func getMockMetadataOne() MetadataType {
	return mockMetadata{
		randomSuffix: "_mocksuffix_one",
	}
}

func getMockMetadataTwo() MetadataType {
	return mockMetadata{
		randomSuffix: "_mocksuffix_two",
	}
}

func (m mockMetadata) GetSuffix() string {
	return m.randomSuffix
}

func (m mockMetadata) Movable() bool {
	return false
}

type mockMetadataFactoryMovable struct{}

func (f mockMetadataFactoryMovable) Create(suffix string) MetadataType {
	if strings.HasSuffix(suffix, getMockMetadataMovable().GetSuffix()) {
		return getMockMetadataMovable()
	}
	return nil
}

type mockMetadataMovable struct {
	randomSuffix string
}

func getMockMetadataMovable() MetadataType {
	return mockMetadataMovable{
		randomSuffix: "_mocksuffix_movable",
	}
}

func (m mockMetadataMovable) GetSuffix() string {
	return m.randomSuffix
}

func (m mockMetadataMovable) Movable() bool {
	return true
}
