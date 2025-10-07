# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.24.3-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG VERSION=dev
ARG GIT_COMMIT=dirty
ARG BUILD_DATE=unknown
ENV CGO_ENABLED=0

RUN --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w \
    -X main.version=$VERSION \
    -X main.commit=$GIT_COMMIT \
    -X main.buildDate=$BUILD_DATE" \
    -o /out/certslurpctl ./cmd/certslurpctl

RUN --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w \
    -X main.version=$VERSION \
    -X main.commit=$GIT_COMMIT \
    -X main.buildDate=$BUILD_DATE" \
    -o /out/certslurpd ./cmd/certslurpd


FROM cgr.dev/chainguard/static:latest AS base
ARG VERSION GIT_COMMIT BUILD_DATE

LABEL org.opencontainers.image.vendor="Charlton Trezevant" \
    org.opencontainers.image.title="certslurp" \
    org.opencontainers.image.source="https://github.com/chtzvt/certslurp" \
    org.opencontainers.image.version=$VERSION \
    org.opencontainers.image.revision=$GIT_COMMIT \
    org.opencontainers.image.created=$BUILD_DATE

# certslurpctl (CLI)
FROM base AS certslurpctl
WORKDIR /home/nonroot
COPY --from=build /out/certslurpctl /usr/local/bin/certslurpctl
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/certslurpctl"]

# certslurp (head/worker)
FROM base AS certslurp
WORKDIR /home/nonroot
COPY --from=build /out/certslurpd /usr/local/bin/certslurpd
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/certslurpd"]

# certslurpd-debug (optional)
# Includes useful ops/debugging tools
FROM alpine:latest AS certslurp-debug
RUN adduser -D -g '' certslurp && apk add --no-cache ca-certificates curl busybox-extras
WORKDIR /home/certslurp
COPY --from=build /out/certslurpd /usr/local/bin/certslurpd
COPY --from=build /out/certslurpctl /usr/local/bin/certslurpctl
USER certslurp
CMD ["/bin/sh"]
