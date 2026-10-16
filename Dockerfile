FROM rust:1.83-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/bt-scraper /usr/local/bin/bt-scraper
COPY config/default.toml /etc/bt-scraper/config.toml
RUN mkdir -p /data && cp /etc/bt-scraper/config.toml /data/config.toml
VOLUME ["/data"]
EXPOSE 8080/tcp 6881/udp
ENV RUST_LOG=bt_scraper=info
ENTRYPOINT ["bt-scraper"]
CMD ["run", "--config", "/data/config.toml"]
