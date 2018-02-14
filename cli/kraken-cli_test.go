package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var update = flag.Bool("update", false, "update .golden files")

func helperTest(t *testing.T, cmd []string) int {
	ci := OriginContentFixture()

	origins, appConfig := OriginFixture(ci.OriginContentItems, []int{10, 10, 10})
	defer func() {
		for _, o := range origins {
			o.Close()
		}
	}()

	buf := new(bytes.Buffer)
	for _, o := range origins {
		go o.Serve()
	}

	errCode := RunMain(cmd, appConfig, buf)
	fmt.Println(buf.String())

	actual := buf.Bytes()
	golden := filepath.Join("testdata", t.Name()+".golden")

	if *update {
		err := ioutil.WriteFile(golden, actual, 0644)
		if err != nil {
			t.Error(err)
		}
	}
	expected, _ := ioutil.ReadFile(golden)

	assert.Equal(t, expected, actual)
	return errCode
}

func TestInfoCommandAllOrigins(t *testing.T) {
	errCode := helperTest(t, []string{"test", "-info", ""})
	assert.Equal(t, errCode, 0)
}

func TestInfoCommandSingleOrigin(t *testing.T) {
	errCode := helperTest(t, []string{"test", "-info", "", "-origin", "origin0"})
	assert.Equal(t, errCode, 0)
}

func TestContentListCommandAllOrigins(t *testing.T) {
	errCode := helperTest(t, []string{"test", "-list", ""})
	assert.Equal(t, errCode, 0)
}

func TestContentListCommandSingleOrigin(t *testing.T) {
	errCode := helperTest(t, []string{"test", "-list", "", "-origin", "origin1"})
	assert.Equal(t, errCode, 0)
}

func TestDeleteContentOK(t *testing.T) {
	ci := OriginContentFixture()
	errCode := helperTest(t, []string{"test", "-delete",
		ci.OriginContentItems[0].Digest})
	assert.Equal(t, errCode, 0)
}

func TestDeleteContentFail(t *testing.T) {
	errCode := helperTest(t, []string{"test", "-delete", ""})
	assert.Equal(t, errCode, 1)
}

func TestRepairContentOk(t *testing.T) {
	ci := OriginContentFixture()

	origins, appConfig := OriginFixture(ci.OriginContentItems, []int{10, 10, 10})
	defer func() {
		for _, o := range origins {
			o.Close()
		}
	}()

	buf := new(bytes.Buffer)
	for _, o := range origins {
		go o.Serve()
	}

	// first remove content on one of the origins
	errCode := RunMain([]string{"test", "-delete",
		ci.OriginContentItems[0].Digest, "-origin", "origin1"}, appConfig, buf)

	assert.Equal(t, errCode, 0)

	// then repair it there
	errCode = RunMain([]string{"test", "-repair",
		ci.OriginContentItems[0].Digest, "-origin", "origin1"}, appConfig, buf)

	assert.Equal(t, errCode, 0)

	fmt.Println(buf.String())

	actual := buf.Bytes()
	golden := filepath.Join("testdata", t.Name()+".golden")

	if *update {
		err := ioutil.WriteFile(golden, actual, 0644)
		if err != nil {
			t.Error(err)
		}
	}
	expected, _ := ioutil.ReadFile(golden)

	assert.Equal(t, expected, actual)
}

func TestRepairContentFail(t *testing.T) {
	errCode := helperTest(t, []string{"test", "-repair", ""})
	assert.Equal(t, errCode, 1)
}
