#!/bin/bash
mvn -B \
-DskipNexusStagingDeployMojo=false \
-DaltDeploymentRepository="libs-release-local::default::http://127.0.1.1:8082/artifactory/libs-release-local" \
-DaltReleaseDeploymentRepository="libs-release-local::default::http://127.0.1.1:8082/artifactory/libs-release-local" \
-DaltSnapshotDeploymentRepository="libs-snapshot-local::default::http://127.0.1.1:8082/artifactory/libs-snapshot-local" \
clean deploy:deploy
