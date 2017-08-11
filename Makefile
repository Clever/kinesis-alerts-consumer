include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

APP_NAME := kinesis-alerts-consumer
SHELL := /bin/bash
PKGS := $(shell go list ./... | grep -v /vendor )
.PHONY: download_jars run build
$(eval $(call golang-version-check,1.8))

all: test build

$(GOPATH)/bin/glide:
	@go get github.com/Masterminds/glide

install_deps: $(GOPATH)/bin/glide
	@$(GOPATH)/bin/glide install

build:
	CGO_ENABLED=0 go build -a -installsuffix cgo -o kinesis-consumer

docker_build:
	GOOS=linux GOARCH=amd64 make build
	docker build -t $(APP_NAME) .

run: docker_build
	@docker run \
	-v /tmp:/tmp \
	-v $(AWS_SHARED_CREDENTIALS_FILE):$(AWS_SHARED_CREDENTIALS_FILE) \
	--env-file=<(echo -e $(_ARKLOC_ENV_FILE)) $(APP_NAME)

test: $(PKGS)
$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)
