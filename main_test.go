package main

import (
	_ "net/http/pprof"
	"testing"
)

func Test_main(t *testing.T) {
	run("./conf/local/config.toml", "./logs")

	select {}
}
