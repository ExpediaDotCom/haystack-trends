.PHONY: all span-timeseries-transformer timeseries-aggregator release

PWD := $(shell pwd)

clean:
	mvn clean

build: clean
	mvn install package

all: clean  span-timeseries-transformer timeseries-aggregator  report-coverage


report-coverage:
	mvn scoverage:report-only


span-timeseries-transformer:
	mvn scoverage:integration-check package -pl span-timeseries-transformer -am


timeseries-aggregator:
	mvn scoverage:integration-check package -pl timeseries-aggregator -am

# build all and release
release: all
	cd span-timeseries-transformer && $(MAKE) release
	cd timeseries-aggregator && $(MAKE) release



