FROM rust:1.88-alpine AS builder

WORKDIR /app

RUN apk add --no-cache musl-dev

COPY . .
RUN cargo build --release

FROM alpine:latest

RUN apk add --no-cache ca-certificates wget

WORKDIR /app

COPY --from=builder /app/target/release/zcloud ./zcloud

EXPOSE 8003

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD wget -qO- http://localhost:8003/health || exit 1

CMD ["./zcloud"]
