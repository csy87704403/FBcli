FROM ubuntu:24.04

ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system freebuff-gateway \
    && useradd --system --gid freebuff-gateway --home-dir /var/lib/freebuff-gateway freebuff-gateway \
    && install -d -o freebuff-gateway -g freebuff-gateway -m 0700 \
       /var/lib/freebuff-gateway /var/lib/freebuff-gateway/accounts \
       /var/lib/freebuff-gateway/default-account /var/lib/freebuff-gateway/tmp \
       /var/lib/freebuff-gateway/work

COPY freebuff-gateway-linux-${TARGETARCH} /opt/freebuff-gateway/freebuff-gateway
COPY freebuff-headless-linux-${TARGETARCH} /opt/freebuff-gateway/freebuff-headless
COPY freebuff-login-linux-${TARGETARCH} /opt/freebuff-gateway/freebuff-login
COPY tree-sitter.wasm /opt/freebuff-gateway/tree-sitter.wasm

RUN chmod 0755 /opt/freebuff-gateway/freebuff-gateway \
    /opt/freebuff-gateway/freebuff-headless \
    /opt/freebuff-gateway/freebuff-login \
    && chmod 0644 /opt/freebuff-gateway/tree-sitter.wasm

ENV HOME=/var/lib/freebuff-gateway \
    TMPDIR=/var/lib/freebuff-gateway/tmp

USER freebuff-gateway
WORKDIR /var/lib/freebuff-gateway/work
VOLUME ["/var/lib/freebuff-gateway"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl --fail --silent http://127.0.0.1:16882/health >/dev/null || exit 1

ENTRYPOINT ["/opt/freebuff-gateway/freebuff-gateway"]
CMD ["-listen", "0.0.0.0:16882", "-admin-listen", "127.0.0.1:16883", "-cli", "/opt/freebuff-gateway/freebuff-headless", "-login-cli", "/opt/freebuff-gateway/freebuff-login", "-cwd", "/var/lib/freebuff-gateway/work", "-state", "/var/lib/freebuff-gateway/freebuff-cli-gateway-state.json", "-accounts-root", "/var/lib/freebuff-gateway/accounts", "-default-account-config", "/var/lib/freebuff-gateway/default-account"]
