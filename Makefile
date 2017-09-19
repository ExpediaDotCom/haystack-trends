.PHONY: all span-timeseries-transformer timeseries-aggregator release

PWD := $(shell pwd)

clean:
	mvn clean

build: clean
	mvn install package

all: clean span-timeseries-transformer timeseries-aggregator

span-timeseries-transformer:
	mvn package -pl span-timeseries-transformer -am
	cd span-timeseries-transformer && $(MAKE) integration_test

timeseries-aggregator:
	mvn package -pl timeseries-aggregator -am
	cd timeseries-aggregator && $(MAKE) integration_test

# build all and release
release: all
	cd span-timeseries-transformer && $(MAKE) release
	cd timeseries-aggregator && $(MAKE) release



