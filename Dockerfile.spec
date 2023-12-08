ARG CRYSTAL_VERSION=1.5.0
FROM 84codes/crystal:${CRYSTAL_VERSION}-alpine

# Create a temporary folder to hold the files
WORKDIR /app

RUN apk upgrade --no-cache \
    && \
    apk add --update --no-cache \
        bash \
        ca-certificates \
        iputils \
    && \
    apk add --no-cache -X http://dl-cdn.alpinelinux.org/alpine/edge/testing watchexec \
    && \
    update-ca-certificates

COPY shard.yml .
COPY shard.lock .

# hadolint ignore=DL3003
RUN shards install --skip-postinstall --skip-executables

COPY scripts/* scripts/

# These provide certificate chain validation where communicating with external services over TLS
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

ENTRYPOINT ["/app/scripts/entrypoint.sh"]