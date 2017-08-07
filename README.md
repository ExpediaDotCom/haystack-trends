# Haystack Span Timeseries Mapper
haystack-span-timeseries-mapper is the module which reads spans and converts them to timeseries datapoints


##Required Reading
 
In order to understand the haystack-span-timeseries-mapper one must be familiar with the [haystack](https://github.com/ExpediaDotCom/haystack) project. Its written in kafka-streams(http://docs.confluent.io/current/streams/index.html) 
and hence some prior knowledge of kafka-streams would be useful.
 


##Technical Details
This specific module reads the data from kafka aggregates them by service-name and span-name and writes out the aggregated time-series metric back to kafka.
This is a simple public static void main application which is written in scala and uses kafka-streams. This is designed to be deployed as a docker container in the expedia ecosystem.


## Building

####Prerequisite: 

* Make sure you have Java 1.8
* Make sure you have maven 3.3.9 or higher
* Make sure you have docker 1.13 or higher
* Make sure you have docker-compose 1.11 or higher


Note : For mac users you can download docker for mac to set you up for the last two steps.


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

####Debugging

1. Open Makefile
2. Add the following arguments to docker run step in `run_integration_test` trigger :
   ```-e SERVICE_DEBUG_ON=true -P 5005:5005```
3. fire `create_integration_test_env`
4. attach a remote debugger on port 5005
5. attach the breakpoints and run the integration test `make run_integration_test` to push data to local kinesis container and start debuggging. 


##The compose file

Docker compose is a tool provided by docker which lets you define multiple services in a single file([docker-compose.yml](https://docs.docker.com/compose/compose-file/)).
All the docker services listed in the compose file and started on the local box when we fire the docker-compose up command. 

We are using docker-compose for our local development and testing since we don't want to have a dependency on a remote kinesis and kafka deployment for validating the functionality of this module
Here's the list of docker services we run to test the app locally
1. Kafka - where the app pushes the data
2. Zookeeper - needed by the kafka container to start up correctly
3. Kinesis - Where the app reads the data from
4. The kinesis-span-collector app running as a docker-service

IMPORTANT:
1. Please set 'AWS_CBOR_DISABLE=1' environment variable before running the app. If you running the app outside docker, 
```
export AWS_CBOR_DISABLE=1

```
2. Run scripts/create-kinesis-stream to create sample stream. Make sure you create the kinesis stream 'haystack-docker-traces'
before you test your app with kinesis' docker image. Also set AWS_CBOR_DISABLE=1 before you run this script. 