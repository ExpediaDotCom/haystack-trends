default: all

# cleanup the workspace
.PHONY: clean
clean:
	mvn clean

# cleanup docker workspace
.PHONY: clean_docker
RUNNING_DOCKER_CONTAINERS = $(shell docker ps -a -q)
ifneq ("$(RUNNING_DOCKER_CONTAINERS)", "")
clean_docker:
	docker stop $(RUNNING_DOCKER_CONTAINERS)
	docker container prune -f
	docker volume prune -f
else
clean_docker:
	docker container prune -f
	docker volume prune -f
endif

# build executable
.PHONY: build
build:
	mvn clean package

# build docker image
DOCKER_IMAGE_TAG := haystack-span-timeseries-mapper
.PHONY: docker_build
docker_build:
	docker build -t $(DOCKER_IMAGE_TAG) -f build/Dockerfile .

# prepare environment for running integration tests
.PHONY: create_integration_test_env
create_integration_test_env: clean_docker
	# run dependecy services : zk
	docker-compose -f build/docker-compose.yml -p sandbox up -d
	sleep 10

PWD := $(shell pwd)
# run integration test against existing environment
.PHONY: run_integration_test
run_integration_test: docker_build
	# run service : run haystack-span-timeseries-mapper and join network of dependecy services
	docker run \
		-d \
		--network=sandbox_default \
		-e AWS_SECRET_KEY=secret \
		-e AWS_ACCESS_KEY=access \
		-e APP_NAME=haystack-span-timeseries-mapper \
		$(DOCKER_IMAGE_TAG)

	# integration tests: running tests in a separate container
	docker run \
		-it \
		--network=sandbox_default \
		-v $(PWD):/src \
		-v ~/.m2:/root/.m2 \
		-w /src \
		-e APP_NAME=haystack-span-timeseries-mapper \
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
REPO := lib/haystack-span-timeseries-mapper
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
ifeq ($(BRANCH), master)
release: all
	docker tag $(DOCKER_IMAGE_TAG) $(REPO):latest
	docker push $(REPO)
else
release: all
endif
