# Flink 2.1.3 with the jars pinned in jars.lock (ADR-002 version matrix).
# Every download is verified against its SHA-256 before it reaches the image.

FROM curlimages/curl:8.23.0 AS jars
COPY jars.lock /tmp/jars.lock
WORKDIR /tmp/lib
RUN grep -vE '^[[:space:]]*(#|$)' /tmp/jars.lock | while read -r sha name url; do \
      curl -fsSL -o "$name" "$url" && echo "$sha  $name" | sha256sum -c - || exit 1; \
    done

FROM flink:2.1.3-scala_2.12-java17
COPY --from=jars --chown=flink:flink /tmp/lib/ /opt/flink/lib/
