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
import lombok.AccessLevel;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jooq.lambda.tuple.Tuple2;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Slf4j
@Command(name = "sink")
@Setter(AccessLevel.PROTECTED)
public class KafkaConnectSinkResetter implements Runnable {
    @Mixin
    private SharedOptions sharedOptions;

    @Option(names = "--delete-consumer-group", description = "Whether to delete the consumer groups")
    private boolean deleteConsumerGroup = false;

    private static Tuple2<TopicPartition, OffsetAndMetadata> setOffsetToZero(
            final TopicPartition topicPartition, final OffsetAndMetadata oldOffsetAndMetadata) {
        log.info("Setting offset for partition {} on topic {} to 0", topicPartition.partition(),
                topicPartition.topic());
        return new Tuple2<>(topicPartition, new OffsetAndMetadata(0));
    }

    private static void resetConsumerGroupOffsets(final Admin adminClient, final String consumerGroupID) {
        final ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult =
                adminClient.listConsumerGroupOffsets(consumerGroupID);
        log.info("Reset consumer group offsets for group {}", consumerGroupID);
        try {
            final Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap =
                    listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();

            final Map<TopicPartition, OffsetAndMetadata> newOffsets = PairSeq.seq(topicPartitionOffsetAndMetadataMap)
                    .mapToPair(KafkaConnectSinkResetter::setOffsetToZero)
                    .toMap();
            adminClient.alterConsumerGroupOffsets(consumerGroupID, newOffsets).all().get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to alter consumer group offsets", e);
        } catch (final ExecutionException e) {
            throw new RuntimeException("Failed to alter consumer group offsets", e);
        }

    }

    private static void deleteConsumerGroup(final Admin adminClient, final String consumerGroupID) {
        final DeleteConsumerGroupsResult deleteConsumerGroupsResult =
                adminClient.deleteConsumerGroups(List.of(consumerGroupID));
        try {
            deleteConsumerGroupsResult.all().get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to delete consumer group", e);
        } catch (final ExecutionException e) {
            throw new RuntimeException("Failed to delete consumer group", e);
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
