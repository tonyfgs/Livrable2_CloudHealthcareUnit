#!/bin/bash
# ============================================================================
# Livrable 2 - CHU - (Re)téléchargement des jars S3A (hadoop-aws + AWS SDK v2)
# ----------------------------------------------------------------------------
# Les jars sont embarqués dans le livrable (dossier ext-jars/) : ce script ne
# sert qu'à les régénérer (reproductibilité). Il est exécuté depuis le dossier
# ext-jars/ (les jars sont téléchargés à côté du script).
#
# Versions : Hadoop 3.4.1 (aligné sur l'image apache/hive:4.2.1) et AWS SDK v2
# 2.24.6 (aws-java-sdk-v2.version du POM hadoop-project:3.4.1).
# ============================================================================
set -euo pipefail

cd "$(dirname "$0")"

MAVEN="https://repo1.maven.org/maven2"
HADOOP_VERSION="3.4.1"
AWS_VERSION="2.24.6"

# Connecteur S3A (Hadoop)
curl -fsSL -O "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"

# AWS SDK v2 (modules individuels — le bundle officiel fait 558 Mo)
AWS_MODULES="
sdk-core
aws-core
auth
regions
profiles
utils
json-utils
identity-spi
endpoints-spi
metrics-spi
http-client-spi
http-auth
http-auth-spi
http-auth-aws
apache-client
checksums
checksums-spi
third-party-jackson-core
s3
kms
sts
protocol-core
aws-xml-protocol
aws-query-protocol
aws-json-protocol
s3-transfer-manager
"
for module in ${AWS_MODULES}; do
  curl -fsSL -O "${MAVEN}/software/amazon/awssdk/${module}/${AWS_VERSION}/${module}-${AWS_VERSION}.jar"
done

# Dépendance du SDK v2 (absente de l'image Hive)
curl -fsSL -O "${MAVEN}/org/reactivestreams/reactive-streams/1.0.4/reactive-streams-1.0.4.jar"

echo "OK : $(ls -1 *.jar | wc -l) jars téléchargés dans $(pwd)"
