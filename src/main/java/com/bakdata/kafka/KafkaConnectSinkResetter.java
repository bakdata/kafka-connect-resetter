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

import com.bakdata.util.seq2.PairSeq;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.jooq.lambda.tuple.Tuple2;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * This command resets or deletes the consumer group of a Kafka Connect sink connector.
 *
 * <pre>{@code
 * Usage: <main class> sink [-hV] [--delete-consumer-group] --brokers=<brokers>
 *                          [--config=<String=String>[,<String=String>...]]...
 *                          <connectorName>
 *       <connectorName>       Connector to reset
 *       --brokers=<brokers>   List of Kafka brokers
 *       --config=<String=String>[,<String=String>...]
 *                             Kafka client and producer configuration properties
 *       --delete-consumer-group
 *                             Whether to delete the consumer group
 *   -h, --help                Show this help message and exit.
 *   -V, --version             Print version information and exit.
 * }</pre>
 */
@Slf4j
@Setter
@Command(name = "sink", mixinStandardHelpOptions = true)
public final class KafkaConnectSinkResetter implements Runnable {
    @Mixin
    private SharedOptions sharedOptions;
    @Option(names = "--delete-consumer-group", description = "Whether to delete the consumer group")
    private boolean deleteConsumerGroup;

    private static Tuple2<TopicPartition, OffsetAndMetadata> setOffsetToZero(
            final TopicPartition topicPartition, final OffsetAndMetadata oldOffsetAndMetadata) {
        log.info("Setting offset for partition {} on topic {} to 0", topicPartition.partition(),
                topicPartition.topic());
        return new Tuple2<>(topicPartition, new OffsetAndMetadata(0));
    }

    private static void resetConsumerGroupOffsets(final Admin adminClient, final String consumerGroupID) {
        final ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult =
                adminClient.listConsumerGroupOffsets(consumerGroupID);
        log.info("Resetting consumer group offsets for group {}", consumerGroupID);
        try {
            final Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap =
                    listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();

            final Map<TopicPartition, OffsetAndMetadata> newOffsets = PairSeq.seq(topicPartitionOffsetAndMetadataMap)
                    .mapToPair(KafkaConnectSinkResetter::setOffsetToZero)
                    .toMap();
            adminClient.alterConsumerGroupOffsets(consumerGroupID, newOffsets).all().get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResetterException("Failed to alter consumer group offsets", e);
        } catch (final ExecutionException e) {
            throw new ResetterException("Failed to alter consumer group offsets", e);
        }
        log.info("Reset consumer group offsets for group {}", consumerGroupID);
    }

    private static void deleteConsumerGroup(final Admin adminClient, final String consumerGroupID) {
        log.info("Deleting consumer group {}", consumerGroupID);
        final DeleteConsumerGroupsResult deleteConsumerGroupsResult =
                adminClient.deleteConsumerGroups(List.of(consumerGroupID));
        try {
            deleteConsumerGroupsResult.all().get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResetterException("Failed to delete consumer group", e);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) {
                log.info("Consumer group {} does not exist, no need to delete it.", consumerGroupID);
                return;
            }
            throw new ResetterException("Failed to delete consumer group", e);
        }
        log.info("Deleted consumer group {}", consumerGroupID);
    }

    @Override
    public void run() {
        final Map<String, Object> properties = this.sharedOptions.createKafkaConfig();
        final Admin adminClient = AdminClient.create(properties);
        final String consumerGroupID = "connect-" + this.sharedOptions.getConnectorName();

        if (this.deleteConsumerGroup) {
            deleteConsumerGroup(adminClient, consumerGroupID);
        } else {
            resetConsumerGroupOffsets(adminClient, consumerGroupID);
        }
    }
}
