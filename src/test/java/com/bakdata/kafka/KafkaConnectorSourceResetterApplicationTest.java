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

import static com.bakdata.kafka.KafkaConnectorSinkResetterApplicationTest.createKafkaCluster;
import static com.bakdata.kafka.KafkaConnectorSinkResetterApplicationTest.getCLI;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.mguenther.kafka.junit.EmbeddedConnect;
import net.mguenther.kafka.junit.EmbeddedConnectConfig;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.ReadKeyValues;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

@ExtendWith(SoftAssertionsExtension.class)
class KafkaConnectorSourceResetterApplicationTest {

    private static final String TOPIC = "topic";
    private static final String CONNECTOR_NAME = "test";
    private static final String OFFSETS = "offsets";
    private final EmbeddedKafkaCluster kafkaCluster = createKafkaCluster();
    @InjectSoftAssertions
    private SoftAssertions softly;
    private EmbeddedConnect connectCluster;

    private static Properties config() {
        final Properties properties = new Properties();
        properties.setProperty(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
        properties.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        properties.setProperty(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.setProperty(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.setProperty(FileStreamSourceConnector.FILE_CONFIG,
                new File(KafkaConnectorSourceResetterApplicationTest.class.getResource("text.txt").getFile())
                        .getAbsolutePath());
        properties.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, TOPIC);
        return properties;
    }

    @AfterEach
    void tearDown() {
        if (this.connectCluster != null) {
            this.connectCluster.stop();
        }
        this.kafkaCluster.stop();
    }

    @BeforeEach
    void setup() {
        this.kafkaCluster.start();
        this.kafkaCluster.createTopic(withName(TOPIC)
                .with(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE)
                .build());
    }

    @Test
    void test() throws InterruptedException {
        this.createConnectCluster();
        delay(10, TimeUnit.SECONDS);
        final List<String> values = this.kafkaCluster.readValues(ReadKeyValues.from(TOPIC)
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .build());
        this.softly.assertThat(values).hasSize(3);
        this.connectCluster.stop();
        delay(10, TimeUnit.SECONDS);
        final KafkaConnectResetterApplication app = new KafkaConnectResetterApplication();

        final CommandLine commandLine = getCLI(app);
        final int exitCode = commandLine.execute("source",
                CONNECTOR_NAME,
                "--brokers", this.kafkaCluster.getBrokerList(),
                "--offset-topic", OFFSETS
        );
        this.softly.assertThat(exitCode).isEqualTo(0);
        this.createConnectCluster();
        delay(10, TimeUnit.SECONDS);
        final List<String> valuesAfterReset = this.kafkaCluster.readValues(ReadKeyValues.from(TOPIC)
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .build());
        this.softly.assertThat(valuesAfterReset).hasSize(6);
    }

    @Test
    void shouldExitOneWhenOffsetTopicIsSetIncorrectly() throws InterruptedException {
        this.createConnectCluster();
        delay(10, TimeUnit.SECONDS);
        final List<String> values = this.kafkaCluster.readValues(ReadKeyValues.from(TOPIC)
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .build());
        this.softly.assertThat(values)
                .hasSize(3);
        this.connectCluster.stop();
        delay(10, TimeUnit.SECONDS);
        final KafkaConnectResetterApplication app = new KafkaConnectResetterApplication();

        final CommandLine commandLine = getCLI(app);
        final int exitCode = commandLine.execute("source",
                CONNECTOR_NAME,
                "--brokers", this.kafkaCluster.getBrokerList(),
                "--offset-topic", "wrong-offset-topic"
        );
        this.softly.assertThat(exitCode)
                .isEqualTo(1);
        this.createConnectCluster();
        delay(10, TimeUnit.SECONDS);
        final List<String> valuesAfterReset = this.kafkaCluster.readValues(ReadKeyValues.from(TOPIC)
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .build());
        this.softly.assertThat(valuesAfterReset)
                .hasSize(3);
    }

    private void createConnectCluster() {
        this.connectCluster = new EmbeddedConnect(EmbeddedConnectConfig.kafkaConnect()
                .with(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, OFFSETS)
                .deployConnector(config())
                .build(), this.kafkaCluster.getBrokerList(), "connect"
        );
        this.connectCluster.start();
    }

}
