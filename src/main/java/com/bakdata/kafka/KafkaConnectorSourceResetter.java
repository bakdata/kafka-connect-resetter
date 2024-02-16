/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/**
 * This command resets the state of a Kafka Connect source connector by sending tombstone messages for each stored Kafka
 * connect offset.
 *
 * <pre>{@code
 * Usage: <main class> source [-hV] --brokers=<brokers>
 *                            --offset-topic=<offsetTopic>
 *                            [--poll-duration=<pollDuration>]
 *                            [--config=<String=String>[,<String=String>...]]...
 *                            <connectorName>
 *       <connectorName>       Connector to reset
 *       --brokers=<brokers>   List of Kafka brokers
 *       --config=<String=String>[,<String=String>...]
 *                             Kafka client and producer configuration properties
 *   -h, --help                Show this help message and exit.
 *       --offset-topic=<offsetTopic>
 *                             Topic where Kafka connect offsets are stored
 *       --poll-duration=<pollDuration>
 *                             Consumer poll duration
 *   -V, --version             Print version information and exit.
 * }</pre>
 *
 * Kafka Connect stores offsets for source connectors in a dedicated topic. The key of such an offset consists of the
 * connector name and a connector specific partition name, e.g.,
 * {@code ["connector-name", { some-source-specific -data... }] }. This tool finds all partitions belonging to the
 * connector that should be reset and deletes the corresponding offsets.
 */

@Slf4j
@Setter
@Command(name = "source", mixinStandardHelpOptions = true)
public final class KafkaConnectorSourceResetter implements Runnable {
    private static final DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendValue(ChronoField.MILLI_OF_SECOND, 3)
            .toFormatter();
    @Mixin
    private SharedOptions sharedOptions;
    @CommandLine.Option(names = "--offset-topic", description = "Topic where Kafka connect offsets are stored",
            required = true)
    private String offsetTopic;

    @CommandLine.Option(names = "--poll-duration", description = "Consumer poll duration")
    private Duration pollDuration = Duration.ofSeconds(10);

    private static Producer<byte[], byte[]> createProducer(final Map<String, Object> kafkaConfig) {
        final Serializer<byte[]> serializer = new ByteArraySerializer();
        return new KafkaProducer<>(kafkaConfig, serializer, serializer);
    }

    private static TopicPartition toTopicPartition(final PartitionInfo partitionInfo) {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }

    private static Converter createConverter(final Map<String, Object> kafkaConfig) {
        final Converter converter = new JsonConverter();
        converter.configure(kafkaConfig, true);
        return converter;
    }

    private static <K, V> List<PartitionInfo> partitionsFor(final Consumer<K, V> consumer, final String topic) {
        final Map<String, List<PartitionInfo>> topicsWithPartition = consumer.listTopics();
        if (!topicsWithPartition.containsKey(topic)) {
            final String message = String.format(
                    "Could not fetch partitions from the offset topic '%s'. Check if the offset topic name is set "
                            + "correctly.",
                    topic);
            throw new IllegalArgumentException(message);
        }
        return topicsWithPartition.get(topic);
    }

    @Override
    public void run() {
        final String id = this.createId();
        final Map<String, Object> kafkaConfig = this.sharedOptions.createKafkaConfig();
        kafkaConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, id);
        kafkaConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, id);
        kafkaConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        final Collection<byte[]> partitions = this.collectPartitions(kafkaConfig);
        log.info("Found {} partitions for connector {}", partitions.size(), this.sharedOptions.getConnectorName());
        this.resetPartitions(partitions, kafkaConfig);
        log.info("Finished resetting {}", this.sharedOptions.getConnectorName());
    }

    private void resetPartitions(final Iterable<byte[]> partitions, final Map<String, Object> kafkaConfig) {
        try (final Producer<byte[], byte[]> producer = createProducer(kafkaConfig)) {
            producer.initTransactions();
            producer.beginTransaction();
            for (final byte[] partition : partitions) {
                final ProducerRecord<byte[], byte[]> producerRecord = this.createResetRecord(partition);
                log.info("Resetting partition {}", new String(partition, StandardCharsets.UTF_8));
                producer.send(producerRecord);
            }
            producer.commitTransaction();
        }
    }

    private Collection<byte[]> collectPartitions(final Map<String, Object> kafkaConfig) {
        final Converter converter = createConverter(kafkaConfig);
        final PartitionCollector collector = PartitionCollector.builder()
                .converter(converter)
                .topicName(this.offsetTopic)
                .connectorName(this.sharedOptions.getConnectorName())
                .build();
        try (final Consumer<byte[], byte[]> consumer = this.createConsumer(kafkaConfig)) {
            ConsumerRecords<byte[], byte[]> records;
            do {
                records = consumer.poll(this.pollDuration);
                records.forEach(consumerRecord -> collector.handle(consumerRecord.key()));
            } while (!records.isEmpty());
            consumer.unsubscribe();
        }
        return collector.getPartitions();
    }

    private String createId() {
        return "kafka-connect-resetter-" + this.sharedOptions.getConnectorName() + "-" + LocalDateTime.now()
                .format(FORMATTER);
    }

    private ProducerRecord<byte[], byte[]> createResetRecord(final byte[] partition) {
        return new ProducerRecord<>(this.offsetTopic, partition, null);
    }

    private Consumer<byte[], byte[]> createConsumer(final Map<String, Object> kafkaConfig) {
        final Deserializer<byte[]> byteArrayDeserializer = new ByteArrayDeserializer();
        final Consumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(kafkaConfig, byteArrayDeserializer, byteArrayDeserializer);
        final List<PartitionInfo> partitions = partitionsFor(consumer, this.offsetTopic);
        final List<TopicPartition> topicPartitions = partitions.stream()
                .map(KafkaConnectorSourceResetter::toTopicPartition)
                .collect(Collectors.toList());
        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);
        return consumer;
    }

}
