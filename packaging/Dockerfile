# A container image of one static binary and an empty data directory.
# Build it with `make image` (it builds the Linux binary first).
#
#   docker run -d --name samsa -p 9092:9092 -v samsa-data:/data \
#       samsa:dev -advertise localhost:9092
#
# Flags after the image name are samsa's. /data is owned by an unprivileged
# user (65532); a bind mount must be too: chown -R 65532:65532 DIR.
FROM scratch
ARG BIN
COPY --chmod=0755 ${BIN} /samsa
COPY --chown=65532:65532 packaging/data /data
USER 65532:65532
VOLUME ["/data"]
# Kafka clients, other nodes, metrics and health (/healthz, /readyz).
EXPOSE 9092 9093 9100
ENTRYPOINT ["/samsa", "-data", "/data", "-listen", "0.0.0.0:9092", "-metrics-listen", "0.0.0.0:9100"]
