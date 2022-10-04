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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

@Slf4j
@Builder
final class PartitionCollector {
    private final Map<String, byte[]> partitions = new HashMap<>();
    private final @NonNull Converter converter;
    private final String topicName;
    private final @NonNull String connectorName;

    void handle(final byte[] bytes) {
        final SchemaAndValue key = this.converter.toConnectData(this.topicName, bytes);
        final Object value = key.value();
        log.debug("Received record {}", value);
        final String connector = ConnectorNameExtractor.extractConnectorName(value);
        if (this.connectorName.equals(connector)) {
            // We use a map to only collect distinct values. A set would not work because byte[] does not
            // override equals and hashCode
            this.partitions.put(new String(bytes, StandardCharsets.UTF_8), bytes);
        }
    }

    Collection<byte[]> getPartitions() {
        return this.partitions.values();
    }
}
