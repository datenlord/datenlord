FROM rust:1.47 as builder
WORKDIR /tmp/build
COPY . .
RUN cargo build --release

FROM ubuntu
LABEL maintainers="DatenLord Authors"
LABEL description="DatenLord Memory Filesystem"

COPY --from=builder /tmp/build/target/release/async_fuse /usr/local/bin/datenlord-fuse
COPY --from=builder /tmp/build/scripts/umount-in-container.sh /usr/local/bin/umount-in-container.sh
ENTRYPOINT ["/bin/sh", "-c", "/usr/local/bin/umount-in-container.sh $FUSE_MOUNT_DIR && /usr/local/bin/datenlord-fuse -m $FUSE_MOUNT_DIR"]
CMD []

