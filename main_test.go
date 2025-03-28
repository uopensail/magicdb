package main

import (
	_ "net/http/pprof"
	"testing"
)

func Test_main(t *testing.T) {
	initConfig("conf/local/config.toml")
	run("./logs")
	select {}
}
