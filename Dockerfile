ARG RUST_IMAGE_VERSION=latest
FROM rust:${RUST_IMAGE_VERSION} as builder
WORKDIR /tmp/build
COPY . .
RUN rustup component add rustfmt # ectd-rs depends on rustfmt
RUN apt-get update && apt-get install -y cmake g++ libprotobuf-dev protobuf-compiler
RUN cargo build --release

FROM ubuntu as csi
LABEL maintainers="DatenLord Authors"
LABEL description="DatenLord CSI Driver"

COPY --from=builder /tmp/build/target/release/csi /usr/local/bin/csiplugin
ENTRYPOINT ["/usr/local/bin/csiplugin"]
CMD []

FROM ubuntu as fuse
LABEL maintainers="DatenLord Authors"
LABEL description="DatenLord Memory Filesystem"

COPY --from=builder /tmp/build/target/release/async_fuse /usr/local/bin/datenlord-fuse
COPY --from=builder /tmp/build/scripts/umount-in-container.sh /usr/local/bin/umount-in-container.sh
COPY --from=builder /tmp/build/scripts/datenlord-entrypoint.sh /usr/local/bin/datenlord-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/datenlord-entrypoint.sh"]
CMD []
