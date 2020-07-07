FROM rust:1.44.1 as builder
 WORKDIR /usr/src/temporal-index
 COPY . .
 RUN cargo install --path .

FROM debian:buster-slim
 RUN apt-get update && apt-get install -y sqlite3 && rm -rf /var/lib/apt/lists/*
 COPY --from=builder /usr/local/cargo/bin/temporal-index /usr/local/bin/temporal-index
 ENV RUST_LOG info
