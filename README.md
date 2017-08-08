# Haystack Span Timeseries Transformer
haystack-span-timeseries-transformer is the module which reads spans and converts them to timeseries datapoints


##Required Reading
 
In order to understand the haystack-span-timeseries-transformers one must be familiar with the [haystack](https://github.com/ExpediaDotCom/haystack) project. Its written in kafka-streams(http://docs.confluent.io/current/streams/index.html) 
and hence some prior knowledge of kafka-streams would be useful.
 


##Technical Details
This specific module reads the data from kafka aggregates them by service-name and span-name and writes out the aggregated time-series metric back to kafka.
This is a simple public static void main application which is written in scala and uses kafka-streams. This is designed to be deployed as a docker container in the expedia ecosystem.


## Building

####Prerequisite: 

* Make sure you have Java 1.8
* Make sure you have maven 3.3.9 or higher
* Make sure you have docker 1.13 or higher




####Build

For a full build, including unit tests, jar + docker image build and integration test, you can run -
```
make all
```

####Integration Test

If you are developing and just want to run integration tests 
```
make integration_test

```