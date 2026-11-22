# Trino with only the Iceberg connector.
# The official image loads all 57 plugins and four sample catalogs (jmx, memory, tpcds,
# tpch) at startup; measured with Native Memory Tracking, their class metadata and compiled
# code were about 440 MB beside a 1.8 GB heap in a 3.2 GB container. The lake uses one
# connector. Same Trino 484, same Iceberg plugin, without the rest.
FROM trinodb/trino:484

USER root
# The image has no find, grep or awk: plain shell only.
RUN for d in /usr/lib/trino/plugin/*; do \
      [ "${d##*/}" = iceberg ] || rm -rf "$d"; \
    done \
 && rm -f /etc/trino/catalog/jmx.properties /etc/trino/catalog/memory.properties \
          /etc/trino/catalog/tpcds.properties /etc/trino/catalog/tpch.properties \
 && set -- /usr/lib/trino/plugin/* \
 && [ "$#" -eq 1 ] && [ "$1" = /usr/lib/trino/plugin/iceberg ]
USER trino
