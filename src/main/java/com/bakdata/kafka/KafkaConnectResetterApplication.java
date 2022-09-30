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

import lombok.AccessLevel;
import lombok.Setter;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * This application resets the Kafka Connect connectors. Available commands are {@code source} and {@code sink}.
 */
@Command(subcommands = {KafkaConnectorSourceResetter.class, KafkaConnectSinkResetter.class})
@Setter(AccessLevel.PRIVATE)
public final class KafkaConnectResetterApplication {
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "print this help and exit")
    private boolean helpRequested;

    /**
     * Run Kafka Connect resetter.
     *
     * @param args program arguments
     */
    public static void main(final String[] args) {
        final int exitCode = new CommandLine(new KafkaConnectResetterApplication()).execute(args);
        System.exit(exitCode);
    }

}
