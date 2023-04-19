# Kafka Connect Resetter Helm Chart

This chart can be used to deploy Kafka Connect Resetter.

## Configuration

You can specify each parameter using the `--set key=value[,key=value]` argument to `helm install`.

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart.

### Job

| Parameter                 | Description                                                                                                            | Default                                    |
|---------------------------|------------------------------------------------------------------------------------------------------------------------|--------------------------------------------|
| `nameOverride`            | The name of the Kubernetes deployment.                                                                                 | `bakdata/kafka-connect-resetter`           |
| `resources`               | See https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/                                     | see [values.yaml](values.yaml) for details |
| `annotations`             | Map of custom annotations to attach to the pod spec.                                                                   | `{}`                                       |
| `labels`                  | Map of custom labels to attach to the pod spec.                                                                        | `{}`                                       |
| `restartPolicy`           | [Restart policy](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy) to use for the job. | `OnFailure`                                |
| `ttlSecondsAfterFinished` | See https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/#ttl-after-finished-controller.         |                                            |

### Image

| Parameter         | Description                                 | Default                          |
|-------------------|---------------------------------------------|----------------------------------|
| `image`           | Docker image of the Kafka producer app.     | `bakdata/kafka-connect-resetter` |
| `imageTag`        | Docker image tag of the Kafka producer app. | `latest`                         |
| `imagePullPolicy` | Docker image pull policy.                   | `Always`                         |

### General

| Parameter          | Description                                                                                                                                | Default  |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `connectorType`    | Type of connector that should be reset. Can be `sink` or `source`. (required)                                                              | `source` |
| `config.brokers`   | Comma separated list of Kafka brokers to connect to.                                                                                       |          |
| `config.connector` | Name of connector to reset. (required)                                                                                                     |          |
| `config.config`    | Configurations for Kafka clients.                                                                                                          | `{}`     |
| `secretRefs`       | Inject existing secrets as environment variables. Map key is used as environment variable name. Value consists of secret `name` and `key`. | `{}`     |

### Sink Connectors

| Parameter                    | Description                          | Default |
|------------------------------|--------------------------------------|---------|
| `config.deleteConsumerGroup` | Whether to delete the consumer group | `false` |

### Source Connectors

| Parameter             | Description                                             | Default   |
|-----------------------|---------------------------------------------------------|-----------|
| `config.offsetTopic`  | Topic where Kafka connect offsets are stored.           | `offsets` |
| `config.pollDuration` | Consumer poll duration.                                 |           |
| `config.converter`    | Converter class used by Kafka Connect to store offsets. |           |
