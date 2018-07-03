#!/bin/bash
cd `dirname $0`/..

if [ -z "$SONATYPE_USERNAME" ]
then
    echo "ERROR! Please set SONATYPE_USERNAME and SONATYPE_PASSWORD environment variable"
    exit 1
fi

if [ -z "$SONATYPE_PASSWORD" ]
then
    echo "ERROR! Please set SONATYPE_PASSWORD environment variable"
    exit 1
fi

if [ ! -z "$GPG_SECRET_KEYS" ]
then
    echo $GPG_SECRET_KEYS | base64 --decode | $GPG_EXECUTABLE --import
fi

if [ ! -z "$GPG_OWNERTRUST" ]
then
    echo $GPG_OWNERTRUST | base64 --decode | $GPG_EXECUTABLE --import-ownertrust
fi

if [ ! -z "${TRAVIS_TAG}" ]
then
    echo "travis tag is set -> updating pom.xml <version> attribute to ${TRAVIS_TAG}"
    mvn clean -Drelease.version=${TRAVIS_TAG} deploy --settings .travis/settings.xml -Dgpg.skip=false -DskipTests=true -B -U
    SUCCESS=$?
else
    echo "no travis tag is set, hence keeping the snapshot version in pom.xml"
    mvn clean deploy --settings .travis/settings.xml -Dgpg.skip=true -DskipTests=true -B -U
    SUCCESS=$?
fi

./mvnw clean deploy --settings .travis/settings.xml -Dgpg.skip=$SKIP_GPG_SIGN -DskipTests=true -B -U

echo "successfully deployed the jars to nexus"