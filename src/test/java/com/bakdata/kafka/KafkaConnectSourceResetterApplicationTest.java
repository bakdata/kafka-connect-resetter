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

import static com.bakdata.kafka.KafkaConnectSinkResetterApplicationTest.getCLI;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

@ExtendWith(SoftAssertionsExtension.class)
class KafkaConnectSourceResetterApplicationTest {

    private static final String TOPIC = "topic";
    private static final String CONNECTOR_NAME = "test";
    private static final String OFFSETS = "offsets";
    private static final Map<String, Object> CONSUMER_PROPERTIES = Map.of(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
    );
    private final EmbeddedConnectCluster connectCluster = new EmbeddedConnectCluster.Builder()
            .name("test-cluster")
            .workerProps(new HashMap<>(Map.of(
                    DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, OFFSETS
            )))
            .numBrokers(3)
            .build();
    @InjectSoftAssertions
    private SoftAssertions softly;

    private static <K, V> List<ConsumerRecord<K, V>> readAll(final Consumer<K, V> consumer, final String topic,
            final Duration timeout) {
        final List<PartitionInfo> partitionInfos = consumer.listTopics().get(topic);
        final List<TopicPartition> topicPartitions = partitionInfos.stream()
                .map(partition -> new TopicPartition(partition.topic(), partition.partition()))
                .toList();
        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);
        return pollAll(consumer, timeout);
    }

    private static Map<String, String> config() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        properties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.put(FileStreamSourceConnector.FILE_CONFIG,
                new File(KafkaConnectSourceResetterApplicationTest.class.getResource("text.txt").getFile())
                        .getAbsolutePath());
        properties.put(FileStreamSourceConnector.TOPIC_CONFIG, TOPIC);
        return properties;
    }

    private static <K, V> List<ConsumerRecord<K, V>> pollAll(final Consumer<K, V> consumer, final Duration timeout) {
        final List<ConsumerRecord<K, V>> records = new ArrayList<>();
        ConsumerRecords<K, V> poll;
        do {
            poll = consumer.poll(timeout);
            poll.forEach(records::add);
        } while (!poll.isEmpty());
        return records;
    }

    @AfterEach
    void tearDown() {
        this.connectCluster.stop();
    }

    @BeforeEach
    void setup() {
        this.connectCluster.start();
        this.connectCluster.kafka().createTopic(TOPIC);
    }

    @Test
    void shouldCorrectlyResetOffsets() throws InterruptedException {
        this.connectCluster.configureConnector(CONNECTOR_NAME, config());
        Thread.sleep(Duration.ofSeconds(10L));
        try (final Consumer<String, String> consumer = this.createConsumer()) {
            final List<ConsumerRecord<String, String>> values = readAll(consumer, TOPIC, Duration.ofSeconds(1L));
            this.softly.assertThat(values)
                    .hasSize(3);
        }
        this.connectCluster.deleteConnector(CONNECTOR_NAME);
        Thread.sleep(Duration.ofSeconds(10L));
        final KafkaConnectResetterApplication app = new KafkaConnectResetterApplication();

        final CommandLine commandLine = getCLI(app);
        final int exitCode = commandLine.execute("source",
                CONNECTOR_NAME,
                "--brokers", this.connectCluster.kafka().bootstrapServers(),
                "--offset-topic", OFFSETS
        );
        this.softly.assertThat(exitCode).isEqualTo(0);
        this.connectCluster.configureConnector(CONNECTOR_NAME, config());
        Thread.sleep(Duration.ofSeconds(10L));
        try (final Consumer<String, String> consumer = this.createConsumer()) {
            final List<ConsumerRecord<String, String>> valuesAfterReset =
                    readAll(consumer, TOPIC, Duration.ofSeconds(1L));
            this.softly.assertThat(valuesAfterReset)
                    .hasSize(6);
        }
    }

    @Test
    void shouldExitOneWhenOffsetTopicIsSetIncorrectly() throws InterruptedException {
        this.connectCluster.configureConnector(CONNECTOR_NAME, config());
        Thread.sleep(Duration.ofSeconds(10L));
        try (final Consumer<String, String> consumer = this.createConsumer()) {
            final List<ConsumerRecord<String, String>> values = readAll(consumer, TOPIC, Duration.ofSeconds(1L));
            this.softly.assertThat(values)
                    .hasSize(3);
        }
        this.connectCluster.deleteConnector(CONNECTOR_NAME);
        Thread.sleep(Duration.ofSeconds(10L));
        final KafkaConnectResetterApplication app = new KafkaConnectResetterApplication();

        final CommandLine commandLine = getCLI(app);
        final int exitCode = commandLine.execute("source",
                CONNECTOR_NAME,
                "--brokers", this.connectCluster.kafka().bootstrapServers(),
                "--offset-topic", "wrong-offset-topic"
        );
        this.softly.assertThat(exitCode)
                .isEqualTo(1);
        this.connectCluster.configureConnector(CONNECTOR_NAME, config());
        Thread.sleep(Duration.ofSeconds(10L));
        try (final Consumer<String, String> consumer = this.createConsumer()) {
            final List<ConsumerRecord<String, String>> valuesAfterReset =
                    readAll(consumer, TOPIC, Duration.ofSeconds(1L));
            this.softly.assertThat(valuesAfterReset)
                    .hasSize(3);
        }
    }

    @SuppressWarnings("unchecked") // Consumer always uses byte[] although serializer is customizable
    private <K, V> Consumer<K, V> createConsumer(final Map<String, Object> properties) {
        return (Consumer<K, V>) this.connectCluster.kafka()
                .createConsumer(properties);
    }

    private Consumer<String, String> createConsumer() {
        return this.createConsumer(CONSUMER_PROPERTIES);
    }

}
