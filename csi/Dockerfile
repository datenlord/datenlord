FROM ubuntu
LABEL maintainers="DatenLord Authors"
LABEL description="DatenLord CSI Driver"
# TODO: use release version
ARG binary=csi

COPY ${binary} /usr/local/bin/csiplugin
ENTRYPOINT ["/usr/local/bin/csiplugin"]
CMD ["--endpoint", "unix:///tmp/both.sock", "--workerport", "50051", "--nodeid", "localhost", "--runas", "both", "--etcd", "http://127.0.0.1:2379", "--datadir", "/tmp/datenlord-data-dir"]

