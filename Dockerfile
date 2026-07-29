FROM golang:1.24 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/block-ingestor ./cmd/block-ingestor && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/block-writer ./cmd/block-writer && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/event-parser ./cmd/event-parser

FROM gcr.io/distroless/static-debian12:nonroot AS block-ingestor
COPY --from=builder /out/block-ingestor /block-ingestor
ENTRYPOINT ["/block-ingestor"]

FROM gcr.io/distroless/static-debian12:nonroot AS block-writer
COPY --from=builder /out/block-writer /block-writer
ENTRYPOINT ["/block-writer"]

FROM gcr.io/distroless/static-debian12:nonroot AS event-parser
COPY --from=builder /out/event-parser /event-parser
ENTRYPOINT ["/event-parser"]
