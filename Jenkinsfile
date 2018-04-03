#!groovy
@Library('devops/pipeline-shared-library@v1-latest') _

// See full documentation how to configure pipeline: https://github.homeawaycorp.com/DevOps/pipeline-shared-library#mavenjavalibrarytemplate
properties([
        buildDiscarder(logRotator(numToKeepStr: '5')),
        disableConcurrentBuilds()
])

mavenJavaLibraryTemplate (
    githubOrganization: 'soa-platform'
)
