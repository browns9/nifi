#!/bin/bash
mvn -B \
-DskipNexusStagingDeployMojo=true \
-DaltDeploymentRepository="libs-release-local::default::http://10.0.2.2:8082/artifactory/libs-release-local" \
-DaltReleaseDeploymentRepository="libs-release-local::default::http://10.0.2.2:8082/artifactory/libs-release-local" \
-DaltSnapshotDeploymentRepository="libs-snapshot-local::default::http://10.0.2.2:8082/artifactory/libs-snapshot-local" \
-DskipTests=true \
help:active-profiles clean deploy $*
