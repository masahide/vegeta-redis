#!/bin/sh

OS=linux
ARCH=amd64
VERSION="$(git describe --tags $1)"
BIN=vegeta-redis
GOOS=$OS CGO_ENABLED=0 GOARCH=$ARCH go build -ldflags "-X main.Version=$VERSION" -o BIN
echo $BIN
