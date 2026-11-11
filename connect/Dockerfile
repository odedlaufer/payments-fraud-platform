# Kafka Connect worker: Debezium Postgres connector + Confluent Avro converter.
# The Debezium image ships only Apicurio converters; Redpanda's Schema Registry
# speaks the Confluent API, so the Confluent converter is added here.

FROM curlimages/curl:8.23.0 AS avro-converter
ARG AVRO_CONVERTER_VERSION=8.3.2
ARG AVRO_CONVERTER_SHA256=f849f49ae500d1c02f94a50d1dd4b3e63d26433843007ac114a7f8e9fb3d6be8
WORKDIR /tmp
RUN curl -fsSL -o avro.zip \
      "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${AVRO_CONVERTER_VERSION}/confluentinc-kafka-connect-avro-converter-${AVRO_CONVERTER_VERSION}.zip" \
 && echo "${AVRO_CONVERTER_SHA256}  avro.zip" | sha256sum -c - \
 && unzip -q avro.zip \
 && mv "confluentinc-kafka-connect-avro-converter-${AVRO_CONVERTER_VERSION}/lib" /tmp/avro-lib

FROM quay.io/debezium/connect:3.7.0.Final
COPY --from=avro-converter --chown=kafka:kafka /tmp/avro-lib/ /kafka/connect/kafka-connect-avro-converter/
