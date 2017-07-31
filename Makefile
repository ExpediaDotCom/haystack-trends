default: all

# cleanup the workspace
.PHONY: clean
clean:
	mvn clean

# build executable
.PHONY: build
build:
	mvn clean package

# build docker image
DOCKER_IMAGE_TAG := kinesis-span-collector
.PHONY: docker_build
docker_build:
	docker build -t $(DOCKER_IMAGE_TAG) .

# prepare environment for running integration tests
.PHONY: create_integration_test_env
create_integration_test_env: docker_build
	# run dependent services : zk, kafka, kinesis and dynamo
	docker-compose -p sandbox up -d
	sleep 10

	# setup before running service : create stream in kinesis and create tabe in dyanmo
	export AWS_CBOR_DISABLE=1
	npm install  --prefix ./scripts/ ./scripts/
	node scripts/create-kinesis-stream.js
	node scripts/create-dynamo-table.js

	# run service : run kinesis-span-collector and join network of dependent services
	docker run \
		-d \
		--network=sandbox_default \
		-e AWS_SECRET_KEY=secret \
		-e AWS_ACCESS_KEY=access \
		-e APP_NAME=kinesis-span-collector \
		-e EXPEDIA_ENVIRONMENT=docker \
		-e AWS_CBOR_DISABLE=1 \
		$(DOCKER_IMAGE_TAG)

PWD := $(shell pwd)
run_integration_test:
	# integration tests: running tests in a separate container
	docker run \
		-it \
		--network=sandbox_default \
		-v $(PWD):/src \
		-v ~/.m2:/root/.m2 \
		-v /Users/vsen/Applications/kafka_2.11-0.11.0.0:/kafka \
		-w /src \
		-e AWS_SECRET_KEY=secret \
		-e AWS_ACCESS_KEY=access \
		-e APP_NAME=kinesis-span-collector \
		-e EXPEDIA_ENVIRONMENT=docker \
		-e AWS_CBOR_DISABLE=1 \
		maven:alpine \
		mvn test -P integration-tests

# integration testing
.PHONY: integration-test
integration_test: create_integration_test_env run_integration_test

# build all
.PHONY: all
all: build integration_test

# build all and release
.PHONY: release
REPO := lib/kinesis-span-collector
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
ifeq ($(BRANCH), master)
release: all
	docker tag $(DOCKER_IMAGE_TAG) $(REPO):latest
	docker push $(REPO)
else
release: all
endif
