# ext-jars — connecteur S3A (Hadoop) pour HiveServer2

L'image `apache/hive:4.2.1` embarque Hadoop 3.4.1 mais **pas** le connecteur
S3A (`hadoop-aws`) ni le SDK AWS : ils sont nécessaires à HiveServer2
(`chu-hive-hs2`) pour exécuter le DDL (warehouse `s3a://lake/warehouse`,
tables externes `LOCATION 's3a://lake/gold/...'`).

## Fonctionnement

Le service `hive-hs2` (docker-compose.yml) monte ce dossier sur
`/tmp/ext-jars` (lecture seule). Au démarrage, le *dynamic jar loader* de
l'entrypoint de l'image (`/entrypoint.sh`) copie automatiquement tous les
`*.jar` de `/tmp/ext-jars` vers `/opt/hive/lib/` — ils sont alors dans le
classpath de HiveServer2. **Aucune action manuelle n'est requise.**

## Contenu (28 jars, ~10 Mo)

| Jar | Rôle |
|-----|------|
| `hadoop-aws-3.4.1.jar` | Connecteur S3A (`org.apache.hadoop.fs.s3a.S3AFileSystem`) — version alignée sur le Hadoop 3.4.1 de l'image |
| `sdk-core`, `aws-core`, `auth`, `regions`, `profiles`, `utils`, `json-utils`, `identity-spi`, `endpoints-spi`, `metrics-spi`, `http-client-spi`, `http-auth`, `http-auth-spi`, `http-auth-aws`, `apache-client`, `checksums`, `checksums-spi`, `third-party-jackson-core` (tous `software.amazon.awssdk` **2.24.6**) | AWS SDK for Java **v2** — Hadoop 3.4.x a migré de AWS SDK v1 (`com.amazonaws`) vers v2 (`software.amazon.awssdk`) ; version 2.24.6 = celle référencée par le POM `hadoop-project:3.4.1` (`aws-java-sdk-v2.version`) |
| `s3`, `kms`, `sts` (2.24.6) | Services S3, KMS (chiffrement SSE-KMS), STS (providers de credentials par rôle IAM) |
| `protocol-core`, `aws-xml-protocol`, `aws-query-protocol`, `aws-json-protocol` (2.24.6) | Protocoles de sérialisation (S3/KMS = XML, STS = query, core = JSON) |
| `s3-transfer-manager` (2.24.6) | Transfer manager S3 (copies/multipart utilisées par S3A) |
| `reactive-streams-1.0.4.jar` | `org.reactivestreams` (dépendance du SDK v2, absente de l'image) |

> Le *bundle* officiel `software.amazon.awssdk:bundle:2.24.6` (558 Mo) a été
> écarté au profit des modules individuels (~10 Mo) : mêmes classes, empreinte
> livrable réduite. La liste ci-dessus est **testée** : le DDL
> (`CREATE DATABASE gold`, `CREATE EXTERNAL TABLE ... LOCATION s3a://...`)
> s'exécute jusqu'à l'appel S3 (validé hors MinIO : aucune
> `ClassNotFoundException`/`NoClassDefFoundError`).

## Re-télécharger les jars (reproductibilité)

```bash
cd scripts/config/hive/ext-jars
./download-jars.sh
```

Nécessite un accès à Maven Central (`repo1.maven.org`).
