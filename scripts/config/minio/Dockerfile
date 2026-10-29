# ============================================================================
# Image MinIO construite FROM SOURCE depuis le tag officiel GitHub.
#
# Pourquoi : les pulls des repositories contenant "minio" (docker.io/minio/*,
# quay.io/minio/*, bitnami/minio) sont refusés par certains daemons/proxys
# ("pull access denied ... repository does not exist"), et dl.min.io ne sert
# plus les binaires en téléchargement direct (HTTP 410). Le code source
# officiel (github.com/minio/minio) reste joignable : on compile le serveur
# MinIO (même code que l'image Docker Hub minio/minio) dans un build
# multi-stage. Aucun pull d'image "minio" n'est donc nécessaire.
#
# Build : ~5-10 min la première fois (clone + compilation Go).
# ============================================================================
FROM golang:1.25-alpine AS build
RUN apk add --no-cache git build-base
WORKDIR /src
RUN git clone --depth 1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git .
# -p 2 / GOMAXPROCS=2 : limite la consommation mémoire du build (daemon à ~8 Go)
RUN GOMAXPROCS=2 CGO_ENABLED=0 go build -p 2 -trimpath -o /out/minio .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/minio /usr/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
