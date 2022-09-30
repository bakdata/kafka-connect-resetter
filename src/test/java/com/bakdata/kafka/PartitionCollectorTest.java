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


import java.util.Map;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class PartitionCollectorTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    private static Converter createJsonConverter() {
        final Converter converter = new JsonConverter();
        converter.configure(Map.of(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false), false);
        return converter;
    }

    @Test
    void shouldRecognizeConnector() {
        final PartitionCollector collector = PartitionCollector.builder()
                .converter(createJsonConverter())
                .connectorName("my-connector")
                .build();
        collector.handle("[\"my-connector\", {}]".getBytes());
        this.softly.assertThat(collector.getPartitions())
                .hasSize(1)
                .containsExactlyInAnyOrder("[\"my-connector\", {}]".getBytes());
    }

    @Test
    void shouldRecognizeDifferentPartitions() {
        final PartitionCollector collector = PartitionCollector.builder()
                .converter(createJsonConverter())
                .connectorName("my-connector")
                .build();
        collector.handle("[\"my-connector\", 1]".getBytes());
        collector.handle("[\"my-connector\", 2]".getBytes());
        this.softly.assertThat(collector.getPartitions())
                .hasSize(2)
                .containsExactlyInAnyOrder("[\"my-connector\", 1]".getBytes(), "[\"my-connector\", 2]".getBytes());
    }

    @Test
    void shouldIgnoreDuplicates() {
        final PartitionCollector collector = PartitionCollector.builder()
                .converter(createJsonConverter())
                .connectorName("my-connector")
                .build();
        collector.handle("[\"my-connector\", {}]".getBytes());
        this.softly.assertThat(collector.getPartitions())
                .hasSize(1)
                .containsExactlyInAnyOrder("[\"my-connector\", {}]".getBytes());
        collector.handle("[\"my-connector\", {}]".getBytes());
        this.softly.assertThat(collector.getPartitions())
                .hasSize(1)
                .containsExactlyInAnyOrder("[\"my-connector\", {}]".getBytes());
    }

    @Test
    void shouldIgnoreDifferentConnectors() {
        final PartitionCollector collector = PartitionCollector.builder()
                .converter(createJsonConverter())
                .connectorName("my-connector")
                .build();
        collector.handle("[\"not-my-connector\", {}]".getBytes());
        this.softly.assertThat(collector.getPartitions()).isEmpty();
    }

}
