FROM golang:1.24 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/block-ingestor ./cmd/block-ingestor && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/block-writer ./cmd/block-writer && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/event-parser ./cmd/event-parser && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/registry-backfill ./cmd/registry-backfill && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/market-sync ./cmd/market-sync && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/valuation-worker ./cmd/valuation-worker && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/alert-worker ./cmd/alert-worker && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/alert-dispatcher ./cmd/alert-dispatcher && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/query-api ./cmd/query-api && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/wallet-profile-worker ./cmd/wallet-profile-worker && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/gmgn-wallet-sync ./cmd/gmgn-wallet-sync && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/wallet-analytics-worker ./cmd/wallet-analytics-worker && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/flashblocks-worker ./cmd/flashblocks-worker && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/trace-worker ./cmd/trace-worker

FROM gcr.io/distroless/static-debian12:nonroot AS block-ingestor
COPY --from=builder /out/block-ingestor /block-ingestor
ENTRYPOINT ["/block-ingestor"]

FROM gcr.io/distroless/static-debian12:nonroot AS block-writer
COPY --from=builder /out/block-writer /block-writer
ENTRYPOINT ["/block-writer"]

FROM gcr.io/distroless/static-debian12:nonroot AS event-parser
COPY --from=builder /out/event-parser /event-parser
ENTRYPOINT ["/event-parser"]

FROM gcr.io/distroless/static-debian12:nonroot AS registry-backfill
COPY --from=builder /out/registry-backfill /registry-backfill
ENTRYPOINT ["/registry-backfill"]

FROM gcr.io/distroless/static-debian12:nonroot AS market-sync
COPY --from=builder /out/market-sync /market-sync
ENTRYPOINT ["/market-sync"]

FROM gcr.io/distroless/static-debian12:nonroot AS valuation-worker
COPY --from=builder /out/valuation-worker /valuation-worker
ENTRYPOINT ["/valuation-worker"]

FROM gcr.io/distroless/static-debian12:nonroot AS alert-worker
COPY --from=builder /out/alert-worker /alert-worker
ENTRYPOINT ["/alert-worker"]

FROM gcr.io/distroless/static-debian12:nonroot AS alert-dispatcher
COPY --from=builder /out/alert-dispatcher /alert-dispatcher
ENTRYPOINT ["/alert-dispatcher"]

FROM gcr.io/distroless/static-debian12:nonroot AS query-api
COPY --from=builder /out/query-api /query-api
ENTRYPOINT ["/query-api"]

FROM gcr.io/distroless/static-debian12:nonroot AS wallet-profile-worker
COPY --from=builder /out/wallet-profile-worker /wallet-profile-worker
ENTRYPOINT ["/wallet-profile-worker"]

FROM gcr.io/distroless/static-debian12:nonroot AS gmgn-wallet-sync
COPY --from=builder /out/gmgn-wallet-sync /gmgn-wallet-sync
ENTRYPOINT ["/gmgn-wallet-sync"]

FROM gcr.io/distroless/static-debian12:nonroot AS wallet-analytics-worker
COPY --from=builder /out/wallet-analytics-worker /wallet-analytics-worker
ENTRYPOINT ["/wallet-analytics-worker"]

FROM gcr.io/distroless/static-debian12:nonroot AS flashblocks-worker
COPY --from=builder /out/flashblocks-worker /flashblocks-worker
ENTRYPOINT ["/flashblocks-worker"]

FROM gcr.io/distroless/static-debian12:nonroot AS trace-worker
COPY --from=builder /out/trace-worker /trace-worker
ENTRYPOINT ["/trace-worker"]
