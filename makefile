CURDIR:=$(shell pwd)

OS:=$(shell uname | tr '[A-Z]' '[a-z]')
ARCH:=$(shell uname | tr '[A-Z]' '[a-z]')
.PHONY: build clean run

GITCOMMITHASH := $(shell git rev-parse --short HEAD)
GITBRANCHNAME := $(shell git symbolic-ref -q --short HEAD || git describe --tags --exact-match)
GOLDFLAGS += -X handler.__GITCOMMITINFO__=$(GITCOMMITHASH).${GITBRANCHNAME}
GOFLAGS = -ldflags "$(GOLDFLAGS)"

PUBLISHDIR=${CURDIR}/dist
PROJECT_NAME=magicdb

all: dev

clean:
	rm -rf ${PUBLISHDIR}/
	mkdir -pv ${PUBLISHDIR}/conf

build: clean
	go build -o ${PUBLISHDIR}/${PROJECT_NAME} $(GOFLAGS)

pre: build
	cp -aRf conf/$@/* ${PUBLISHDIR}/conf
	cp run.sh ${PUBLISHDIR}/

prod: build
	cp -aRf conf/$@/* ${PUBLISHDIR}/conf
	cp run.sh ${PUBLISHDIR}/

dev: build
	cp -aRf conf/$@/* ${PUBLISHDIR}/conf
	cp run.sh ${PUBLISHDIR}/
test: build
	cp -aRf conf/$@/* ${PUBLISHDIR}/conf
	cp run.sh ${PUBLISHDIR}/
local: build
	cp -aRf conf/$@/* ${PUBLISHDIR}/conf
	cd dist && ./${PROJECT_NAME}