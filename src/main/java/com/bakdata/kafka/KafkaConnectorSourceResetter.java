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

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
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

@ToString
@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Setter(AccessLevel.PROTECTED)
@Command(name = "source")
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
    @CommandLine.Option(names = "--converter", description = "Converter class used by Kafka Connect to store offsets")
    private Class<? extends Converter> converterClass = JsonConverter.class;

    private static Producer<byte[], byte[]> createProducer(final Map<String, Object> kafkaConfig) {
        final Serializer<byte[]> serializer = new ByteArraySerializer();
        return new KafkaProducer<>(kafkaConfig, serializer, serializer);
    }

    private static TopicPartition toTopicPartition(final PartitionInfo partitionInfo) {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
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
                final ProducerRecord<byte[], byte[]> record = this.createResetRecord(partition);
                log.info("Resetting partition {}", new String(partition));
                producer.send(record);
            }
            producer.commitTransaction();
        }
    }

    private Collection<byte[]> collectPartitions(final Map<String, Object> kafkaConfig) {
        final Converter converter = this.createConverter(kafkaConfig);
        final PartitionCollector collector = PartitionCollector.builder()
                .converter(converter)
                .topicName(this.offsetTopic)
                .connectorName(this.sharedOptions.getConnectorName())
                .build();
        try (final Consumer<byte[], byte[]> consumer = this.createConsumer(kafkaConfig)) {
            ConsumerRecords<byte[], byte[]> records;
            do {
                records = consumer.poll(this.pollDuration);
                records.forEach(record -> collector.handle(record.key()));
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

    private Converter createConverter(final Map<String, Object> kafkaConfig) {
        try {
            final Converter converter = this.converterClass.getDeclaredConstructor().newInstance();
            converter.configure(kafkaConfig, true);
            return converter;
        } catch (final InstantiationException | NoSuchMethodException | IllegalAccessException |
                       InvocationTargetException e) {
            throw new RuntimeException("Error creating converter of class " + this.converterClass.getName(), e);
        }
    }

    private Consumer<byte[], byte[]> createConsumer(final Map<String, Object> kafkaConfig) {
        final Deserializer<byte[]> byteArrayDeserializer = new ByteArrayDeserializer();
        final Consumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(kafkaConfig, byteArrayDeserializer, byteArrayDeserializer);
        final List<PartitionInfo> partitions = consumer.partitionsFor(this.offsetTopic);
        final List<TopicPartition> topicPartitions = partitions.stream()
                .map(KafkaConnectorSourceResetter::toTopicPartition)
                .collect(Collectors.toList());
        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);
        return consumer;
    }


}
