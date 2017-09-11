.PHONY: all

export DOCKER_ORG := expediadotcom
export DOCKER_IMAGE_NAME := haystack-span-timeseries-transformer

clean:
	mvn clean

build:  clean
	mvn package

docker_build:
	docker build -t $(DOCKER_IMAGE_NAME) -f build/docker/Dockerfile .

.PHONY: integration_test
integration_test:
	mvn test -P integration-tests

# build jar, docker image and run integration tests
all: build docker_build integration_test

# build all and release
release: all
	./build/docker/publish-to-docker-hub.sh
