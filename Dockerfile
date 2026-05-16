# Build stage
FROM rust:1.80-slim as builder

WORKDIR /usr/src/usagedb
COPY . .

RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /usr/src/usagedb/target/release/usagedb /app/usagedb

# Create data directory
RUN mkdir -p /app/data

# Set environment variables
ENV RUST_LOG=info
ENV USAGEDB_HTTP_BIND_ADDRESS=0.0.0.0:8080

EXPOSE 8080

CMD ["./usagedb"]
