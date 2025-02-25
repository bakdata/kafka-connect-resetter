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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.file.FileStreamSinkConnector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

@ExtendWith(SoftAssertionsExtension.class)
class KafkaConnectSinkResetterApplicationTest {

    private static final String TOPIC = "topic";
    private static final String CONNECTOR_NAME = "test";
    private static final Map<String, Object> PRODUCER_PROPERTIES = Map.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
    );
    @InjectSoftAssertions
    private SoftAssertions softly;
    private final EmbeddedConnectCluster connectCluster = new EmbeddedConnectCluster.Builder()
            .name("test-cluster")
            .build();
    private Admin adminClient;

    static CommandLine getCLI(final KafkaConnectResetterApplication app) {
        final CommandLine commandLine = new CommandLine(app);
        final StringWriter stringWriter = new StringWriter();
        commandLine.setOut(new PrintWriter(stringWriter));
        return commandLine;
    }

    private static Map<String, String> config(final Path tempFile) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSinkConnector.class.getName());
        properties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.put(FileStreamSinkConnector.FILE_CONFIG, tempFile.toString());
        properties.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        return properties;
    }

    @AfterEach
    void tearDown() {
        this.connectCluster.stop();
        if (this.adminClient != null) {
            this.adminClient.close();
        }
    }

    @BeforeEach
    void setup() {
        this.connectCluster.start();
        this.connectCluster.kafka().createTopic(TOPIC);
        this.adminClient = this.createAdminClient();
    }

    @Test
    void shouldDeleteConsumerGroup(@TempDir final File tempDir)
            throws InterruptedException, IOException, ExecutionException {

        final Path tempFile = Files.createFile(tempDir.toPath().resolve("test-delete-consumer-group.txt"));
        this.connectCluster.configureConnector(CONNECTOR_NAME, config(tempFile));
        Thread.sleep(Duration.ofSeconds(10L));

        try (final Producer<String, String> producer = this.createProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, null, "test-1"));
            producer.send(new ProducerRecord<>(TOPIC, null, "test-2"));
        }
        Thread.sleep(Duration.ofSeconds(10L));
        this.connectCluster.deleteConnector(CONNECTOR_NAME);
        this.softly.assertThat(this.adminClient.listConsumerGroups().all().get()).hasSize(1);

        final KafkaConnectResetterApplication app = new KafkaConnectResetterApplication();

        final CommandLine commandLine = getCLI(app);
        this.softly.assertThat(tempFile).hasContent("test-1\ntest-2\n");
        final int exitCode = commandLine.execute("sink",
                CONNECTOR_NAME,
                "--brokers", this.connectCluster.kafka().bootstrapServers(),
                "--delete-consumer-group"
        );
        this.softly.assertThat(exitCode).isEqualTo(0);

        this.softly.assertThat(this.adminClient.listConsumerGroups().all().get()).isEmpty();

        this.connectCluster.configureConnector(CONNECTOR_NAME, config(tempFile));

        Thread.sleep(Duration.ofSeconds(10L));
        this.softly.assertThat(tempFile).hasContent("test-1\ntest-2\ntest-1\ntest-2\n");
    }

    @Test
    void shouldNotFailOnConsumerGroupNotFound(@TempDir final File tempDir)
            throws InterruptedException, IOException, ExecutionException {

        final Path tempFile = Files.createFile(tempDir.toPath().resolve("test-delete-consumer-group.txt"));
        this.connectCluster.configureConnector(CONNECTOR_NAME, config(tempFile));
        Thread.sleep(Duration.ofSeconds(10L));

        this.connectCluster.deleteConnector(CONNECTOR_NAME);
        this.softly.assertThat(this.adminClient.listConsumerGroups().all().get()).hasSize(1);

        final KafkaConnectResetterApplication app = new KafkaConnectResetterApplication();

        final CommandLine commandLine = getCLI(app);
        final int exitCode = commandLine.execute("sink",
                "another-fake-connector", // non-existing connector
                "--brokers", this.connectCluster.kafka().bootstrapServers(),
                "--delete-consumer-group"
        );
        this.softly.assertThat(exitCode).isEqualTo(0);
        this.softly.assertThat(this.adminClient.listConsumerGroups().all().get()).hasSize(1);
    }

    @Test
    void shouldCorrectlyResetOffsets(@TempDir final File tempDir)
            throws InterruptedException, IOException, ExecutionException {
        final Path tempFile = Files.createFile(tempDir.toPath().resolve("test-reset-offsets.txt"));
        this.connectCluster.configureConnector(CONNECTOR_NAME, config(tempFile));
        Thread.sleep(Duration.ofSeconds(10L));

        try (final Producer<String, String> producer = this.createProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, null, "test-1"));
            producer.send(new ProducerRecord<>(TOPIC, null, "test-2"));
        }
        Thread.sleep(Duration.ofSeconds(10L));

        this.softly.assertThat(this.adminClient.listConsumerGroups().all().get()).hasSize(1);
        this.connectCluster.deleteConnector(CONNECTOR_NAME);
        final KafkaConnectResetterApplication app = new KafkaConnectResetterApplication();

        final CommandLine commandLine = getCLI(app);
        this.softly.assertThat(tempFile).hasContent("test-1\ntest-2\n");
        final int exitCode =
                commandLine.execute("sink",
                        CONNECTOR_NAME,
                        "--brokers", this.connectCluster.kafka().bootstrapServers()
                );
        this.softly.assertThat(exitCode).isEqualTo(0);
        this.connectCluster.configureConnector(CONNECTOR_NAME, config(tempFile));

        this.softly.assertThat(this.adminClient.listConsumerGroups().all().get()).hasSize(1);

        Thread.sleep(Duration.ofSeconds(10L));
        this.softly.assertThat(tempFile).hasContent("test-1\ntest-2\ntest-1\ntest-2\n");
    }

    private Admin createAdminClient() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.connectCluster.kafka().bootstrapServers());
        return AdminClient.create(properties);
    }

    @SuppressWarnings("unchecked") // Producer always uses byte[] although serializer is customizable
    private <K, V> Producer<K, V> createProducer(final Map<String, Object> properties) {
        return (Producer<K, V>) this.connectCluster.kafka()
                .createProducer(properties);
    }

    private Producer<String, String> createProducer() {
        return this.createProducer(PRODUCER_PROPERTIES);
    }
}
