FROM rust:1.47 as builder
WORKDIR /tmp/build
COPY . .
RUN cargo build --release

FROM ubuntu
LABEL maintainers="DatenLord Authors"
LABEL description="DatenLord Memory Filesystem"

COPY --from=builder /tmp/build/target/release/async_fuse /usr/local/bin/datenlord-fuse
COPY --from=builder /tmp/build/umount-in-container.sh /usr/local/bin/umount-in-container.sh
ENTRYPOINT ["/usr/local/bin/datenlord-fuse"]
CMD []

