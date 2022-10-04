# Kafka Connect Resetter Helm Repository

## Install

```
helm repo add bakdata-kafka-connect-resetter https://raw.githubusercontent.com/bakdata/kafka-connect-resetter/<branch_name>/charts/
helm install bakdata-kafka-connect-resetter/kafka-connect-resetter
```

## Development

To update the helm repository please run:

```
cd <your-chart-dir>
helm package .
cd ..
helm repo index .
```
