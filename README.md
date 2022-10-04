[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.kafka-connect-resetter?repoName=bakdata%2Fkafka-connect-resetter&branchName=initial)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=34&repoName=bakdata%2Fkafka-connect-resetter&branchName=initial)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Akafka-connect-resetter&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=com.bakdata.kafka%3Akafka-connect-resetter)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Akafka-connect-resetter&metric=coverage)](https://sonarcloud.io/summary/new_code?id=com.bakdata.kafka%3Akafka-connect-resetter)

# kafka-connect-resetter

An application to reset the state of Kafka Connect connectors.

## Usage

### Source resetter

This command resets the state of a Kafka Connect source connector by sending tombstone messages for each stored Kafka
connect offset.

```
Usage: <main class> source [-hV] --brokers=<brokers>
                           --offset-topic=<offsetTopic>
                           [--poll-duration=<pollDuration>]
                           [--config=<String=String>[,<String=String>...]]...
                           <connectorName>
      <connectorName>       Connector to reset
      --brokers=<brokers>   List of Kafka brokers
      --config=<String=String>[,<String=String>...]
                            Kafka client and producer configuration properties
  -h, --help                Show this help message and exit.
      --offset-topic=<offsetTopic>
                            Topic where Kafka connect offsets are stored
      --poll-duration=<pollDuration>
                            Consumer poll duration
  -V, --version             Print version information and exit.
```

### Sink resetter

This command resets or deletes the consumer group of a Kafka Connect sink connector.

```
Usage: <main class> sink [-hV] [--delete-consumer-group] --brokers=<brokers>
                         [--config=<String=String>[,<String=String>...]]...
                         <connectorName>
      <connectorName>       Connector to reset
      --brokers=<brokers>   List of Kafka brokers
      --config=<String=String>[,<String=String>...]
                            Kafka client and producer configuration properties
      --delete-consumer-group
                            Whether to delete the consumer group
  -h, --help                Show this help message and exit.
  -V, --version             Print version information and exit.
```

### Helm Charts

For the configuration and deployment to Kubernetes, you can use
the [Helm Chart](https://github.com/bakdata/kafka-connect-resetter/tree/master/charts).

## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
> git clone git@github.com:bakdata/kafka-connect-resetter.git
> cd kafka-connect-resetter && ./gradlew build
```

Please note, that we have [code styles](https://github.com/bakdata/bakdata-code-styles) for Java.
They are basically the Google style guide, with some small modifications.

## Contributing

We are happy if you want to contribute to this project.
If you find any bugs or have suggestions for improvements, please open an issue.
We are also happy to accept your PRs.
Just open an issue beforehand and let us know what you want to do and why.

## License

This project is licensed under the MIT license.
Have a look at the [LICENSE](https://github.com/bakdata/kafka-connect-resetter/blob/main/LICENSE) for more details.
