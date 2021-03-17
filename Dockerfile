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
ENTRYPOINT ["/bin/sh", "-c", "/usr/local/bin/umount-in-container.sh $FUSE_MOUNT_DIR && exec /usr/local/bin/datenlord-fuse -m $FUSE_MOUNT_DIR --etcd $ETCD_ENDPOINT --volume_info $FUSE_VOLUME_INFO"]
CMD []
