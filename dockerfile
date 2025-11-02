
FROM rust:1-alpine AS builder
WORKDIR /usr/src/app
# Install build dependencies for Rust (C toolchain)
RUN apk add --no-cache build-base pkgconfig musl-dev openssl-dev
COPY . .
RUN cargo build --release

FROM alpine:latest
# Install curl for health checks
RUN apk add --no-cache curl

COPY --from=builder /usr/src/app/target/release/rustOutboxSweeper /usr/local/bin/

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
  CMD curl -f http://localhost:8080/health || exit 1

CMD ["rustOutboxSweeper"]