# Kafka Connect Resetter Helm Repository

## Install

```
helm repo add bakdata-kafka-connect-resetter https://bakdata.github.io/kafka-connect-resetter/
helm install bakdata-kafka-connect-resetter/kafka-connect-resetter/
```

## Development

To update the helm repository please run:

```
cd <your-chart-dir>
helm package .
cd ..
helm repo index .
```
