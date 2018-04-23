.PHONY: all span-timeseries-transformer timeseries-aggregator release

PWD := $(shell pwd)
MAVEN := ./mvnw

clean:
	${MAVEN} clean

build: clean
	${MAVEN} install package

all: clean  span-timeseries-transformer timeseries-aggregator  report-coverage

report-coverage:
	${MAVEN} scoverage:report-only

span-timeseries-transformer:
	${MAVEN} package scoverage:integration-check -pl span-timeseries-transformer -am

timeseries-aggregator:
	${MAVEN} package scoverage:integration-check -pl timeseries-aggregator -am

# build all and release
release: all
	cd span-timeseries-transformer && $(MAKE) release
	cd timeseries-aggregator && $(MAKE) release
	./.travis/deploy.sh



