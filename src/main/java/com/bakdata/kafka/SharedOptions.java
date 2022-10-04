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

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.CommonClientConfigs;
import picocli.CommandLine;

/**
 * Shared CLI options to configure Kafka Connect resetters.
 */
@Getter
@Setter
public class SharedOptions {
    @CommandLine.Parameters(index = "0", description = "Connector to reset")
    private String connectorName;
    @CommandLine.Option(names = "--brokers", description = "List of Kafka brokers", required = true)
    private String brokers;
    @CommandLine.Option(names = "--config", description = "Kafka client and producer configuration properties",
            split = ",")
    private Map<String, String> config = new HashMap<>();

    Map<String, Object> createKafkaConfig() {
        final Map<String, Object> properties = new HashMap<>(this.config);
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.brokers);
        return properties;
    }

}
