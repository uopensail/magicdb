.PHONY: build clean run

GITHASH := $(shell git rev-parse --short HEAD)
GOLDFLAGS += -X app.__GITHASH__=$(GITHASH)
GOFLAGS = -ldflags "$(GOLDFLAGS)"

all: build-dev
build: clean
	go build -o build/magicdb-server $(GOFLAGS)
build-dev: build
	cp -rf conf/dev build/conf
build-prod: build
	cp -rf conf/prod build/conf
run: build-dev
	./build/pangu-server -config="./build/conf"
clean:
	rm -rf ./build