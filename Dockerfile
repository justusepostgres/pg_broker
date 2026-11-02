FROM rust:1.92.0-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev libssl-dev pkg-config libreadline-dev zlib1g-dev bison flex ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /tmp
ADD https://ftp.postgresql.org/pub/source/v17.5/postgresql-17.5.tar.bz2 /tmp/postgresql.tar.bz2
RUN echo 'fcb7ab38e23b264d1902cb25e6adafb4525a6ebcbd015434aeef9eda80f528d8  postgresql.tar.bz2' | sha256sum -c - && tar -xjf postgresql.tar.bz2 && cd postgresql-17.5 && ./configure --prefix=/opt/pgsql --without-icu --without-readline --without-zlib && make -j2 && make install
ENV PGRX_PG_CONFIG_PATH=/opt/pgsql/bin/pg_config
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY scripts ./scripts
RUN cargo build --workspace --locked && PG_CONFIG=/opt/pgsql/bin/pg_config scripts/install-extension.sh

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 libssl3 && rm -rf /var/lib/apt/lists/* && useradd -m -u 10001 broker
COPY --from=build /opt/pgsql /opt/pgsql
COPY --from=build /src/target/debug/pg-broker-gateway /usr/local/bin/
COPY scripts/container-db.sh /usr/local/bin/
ENV PATH=/opt/pgsql/bin:$PATH
RUN mkdir /data && chown broker:broker /data
USER broker
CMD ["pg-broker-gateway"]

# CNPG supplies its own instance manager and expects PostgreSQL's standard
# user, paths and locales. Only the extension artifacts enter this image.
FROM ghcr.io/cloudnative-pg/postgresql:17.5-bookworm@sha256:89e09df1966a124cd8e015d16f8e888809e5560a3df2900c91433f73176a0b2d AS cnpg
COPY --from=build /opt/pgsql/lib/pg_broker.so /usr/lib/postgresql/17/lib/pg_broker.so
COPY --from=build /src/target/extension/pg_broker/ /usr/share/postgresql/17/extension/

FROM debian:bookworm-slim AS gateway
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/* && useradd -m -u 10001 broker
COPY --from=build /src/target/debug/pg-broker-gateway /usr/local/bin/
USER broker
CMD ["pg-broker-gateway"]
