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

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;
import static net.mguenther.kafka.junit.EmbeddedKafkaConfig.brokers;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaConfig$;
import net.mguenther.kafka.junit.EmbeddedConnect;
import net.mguenther.kafka.junit.EmbeddedConnectConfig;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.SendValues;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.connect.file.FileStreamSinkConnector;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.StringConverter;
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
class KafkaConnectorSinkResetterApplicationTest {

    private static final String TOPIC = "topic";
    private static final String CONNECTOR_NAME = "test";
    private static final String OFFSETS = "offsets";
    private final EmbeddedKafkaCluster kafkaCluster = createKafkaCluster();
    @InjectSoftAssertions
    private SoftAssertions softly;
    private EmbeddedConnect connectCluster;
    private Admin adminClient;

    static EmbeddedKafkaCluster createKafkaCluster() {
        return provisionWith(newClusterConfig()
                .configure(brokers()
                        // FIXME Since Kafka 3.0, there is a problem with the cleanup policy of the connect offset
                        //  topic
                        //  https://github.com/mguenther/kafka-junit/issues/70
                        .with(KafkaConfig$.MODULE$.LogCleanupPolicyProp(), "compact")
                )
        );
    }

    static CommandLine getCLI(final KafkaConnectResetterApplication app) {
        final CommandLine commandLine = new CommandLine(app);
        final StringWriter stringWriter = new StringWriter();
        commandLine.setOut(new PrintWriter(stringWriter));
        return commandLine;
    }

    private static Properties config(final Path tempFile) {
        final Properties properties = new Properties();
        properties.setProperty(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
        properties.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSinkConnector.class.getName());
        properties.setProperty(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.setProperty(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.setProperty(FileStreamSinkConnector.FILE_CONFIG, tempFile.toString());
        properties.setProperty(SinkConnector.TOPICS_CONFIG, TOPIC);
        return properties;
    }

    @AfterEach
    void tearDown() {
        if (this.connectCluster != null) {
            this.connectCluster.stop();
        }
        if (this.adminClient != null) {
            this.adminClient.close();
        }
        this.kafkaCluster.stop();
    }

    @BeforeEach
    void setup() {
        this.kafkaCluster.start();
        this.kafkaCluster.createTopic(withName(TOPIC)
                .with(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE)
                .build());
        this.adminClient = this.createAdminClient();
    }

    @Test
    void shouldDeleteConsumerGroup(@TempDir final File tempDir)
            throws InterruptedException, IOException, ExecutionException {

        final Path tempFile = Files.createFile(tempDir.toPath().resolve("test-delete-consumer-group.txt"));
        this.createConnectCluster(tempFile);
        delay(10, TimeUnit.SECONDS);

        this.kafkaCluster.send(SendValues.to(TOPIC, "test-1", "test-2").build());
        delay(10, TimeUnit.SECONDS);
        this.connectCluster.stop();
        this.softly.assertThat(this.adminClient.listConsumerGroups().all().get()).hasSize(1);

        final KafkaConnectResetterApplication app = new KafkaConnectResetterApplication();

        final CommandLine commandLine = getCLI(app);
        this.softly.assertThat(tempFile).hasContent("test-1\ntest-2\n");
        final int exitCode = commandLine.execute("sink",
                CONNECTOR_NAME,
                "--brokers", this.kafkaCluster.getBrokerList(),
                "--delete-consumer-group"
        );
        this.softly.assertThat(exitCode).isEqualTo(0);

        this.softly.assertThat(this.adminClient.listConsumerGroups().all().get()).isEmpty();

        this.createConnectCluster(tempFile);

        delay(10, TimeUnit.SECONDS);
        this.softly.assertThat(tempFile).hasContent("test-1\ntest-2\ntest-1\ntest-2\n");
    }

    @Test
    void shouldCorrectlyResetOffsets(@TempDir final File tempDir)
            throws InterruptedException, IOException, ExecutionException {
        final Path tempFile = Files.createFile(tempDir.toPath().resolve("test-reset-offsets.txt"));
        this.createConnectCluster(tempFile);
        delay(10, TimeUnit.SECONDS);

        this.kafkaCluster.send(SendValues.to(TOPIC, "test-1", "test-2").build());
        delay(10, TimeUnit.SECONDS);

        this.softly.assertThat(this.adminClient.listConsumerGroups().all().get()).hasSize(1);
        this.connectCluster.stop();
        final KafkaConnectResetterApplication app = new KafkaConnectResetterApplication();

        final CommandLine commandLine = getCLI(app);
        this.softly.assertThat(tempFile).hasContent("test-1\ntest-2\n");
        final int exitCode =
                commandLine.execute("sink",
                        CONNECTOR_NAME,
                        "--brokers", this.kafkaCluster.getBrokerList()
                );
        this.softly.assertThat(exitCode).isEqualTo(0);
        this.createConnectCluster(tempFile);

        this.softly.assertThat(this.adminClient.listConsumerGroups().all().get()).hasSize(1);

        delay(10, TimeUnit.SECONDS);
        this.softly.assertThat(tempFile).hasContent("test-1\ntest-2\ntest-1\ntest-2\n");
    }

    private void createConnectCluster(final Path tempFile) {
        this.connectCluster = new EmbeddedConnect(EmbeddedConnectConfig.kafkaConnect()
                .with(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, OFFSETS)
                .deployConnector(config(tempFile))
                .build(), this.kafkaCluster.getBrokerList(), "connect"
        );
        this.connectCluster.start();
    }

    private Admin createAdminClient() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.kafkaCluster.getBrokerList());
        properties.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        return AdminClient.create(properties);
    }
}
