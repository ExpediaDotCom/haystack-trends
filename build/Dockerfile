FROM docker-registry.tools.expedia.com/stratus/app-java:8


# Configure environment
ENV EXPEDIA_ENVIRONMENT dev
ENV APP_NAME kinesis-span-collector

COPY docker-files/docker-entrypoint.sh /

# Copy the application jar to the app_bin
ADD target/${APP_NAME}.jar ${APP_BIN}/${APP_NAME}.jar

WORKDIR ${APP_HOME}

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["service"]
API Training Shop Blog About
