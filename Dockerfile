FROM rust:1.88-alpine AS builder

WORKDIR /app

RUN apk add --no-cache musl-dev openssl-dev pkgconf

COPY . .
RUN cargo build --release

FROM alpine:latest

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/target/release/zcloud ./zcloud

EXPOSE 8003

CMD ["./zcloud"]
