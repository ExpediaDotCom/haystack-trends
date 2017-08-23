FROM openjdk:8-jre
MAINTAINER Haystack <haystack@expedia.com>

ENV APP_NAME haystack-span-timeseries-transformer
ENV APP_HOME /app/bin

RUN mkdir -p ${APP_HOME}

COPY target/${APP_NAME}.jar ${APP_HOME}/
COPY build/docker/start-app.sh ${APP_HOME}/

WORKDIR ${APP_HOME}

ENTRYPOINT ["./start-app.sh"]
