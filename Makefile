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

build_transformer:
	${MAVEN} package -DfinalName=haystack-span-timeseries-transformer -pl span-timeseries-transformer -am

build_aggregator:
	${MAVEN} package -DfinalName=haystack-timeseries-aggregator -pl timeseries-aggregator -am

# build all and release
release: clean build_transformer build_aggregator
	cd span-timeseries-transformer && $(MAKE) release
	cd timeseries-aggregator && $(MAKE) release
	./.travis/deploy.sh



