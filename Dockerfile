FROM rust:1.85-slim-bookworm AS builder

WORKDIR /app

COPY Cargo.toml ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home app

WORKDIR /app

COPY --from=builder /app/target/release/ferris-market-data-backend /usr/local/bin/ferris-market-data-backend

ENV HOST=0.0.0.0
ENV PORT=8080

EXPOSE 8080

USER app

CMD ["ferris-market-data-backend"]
