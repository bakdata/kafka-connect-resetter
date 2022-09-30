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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConnectorNameExtractorTest {

    static Stream<Arguments> generateConnectorNames() {
        return Stream.of(
                Arguments.of(List.of("foo"), "foo"),
                Arguments.of(List.of("bar", "baz"), "bar"),
                Arguments.of(List.of("foo", 1), "foo")
        );
    }

    static Stream<Arguments> generateInvalidConnectorNames() {
        return Stream.of(
                Arguments.of(List.of(), "Expected at least one element"),
                Arguments.of(List.of(1), "Expected first element to be string but got java.lang.Integer"),
                Arguments.of(newListWithNullElement(), "Expected first element to be string but got null"),
                Arguments.of("foo", "Expected record to be a list but got java.lang.String"),
                Arguments.of(null, "Expected record to be a list but got null")
        );
    }

    private static List<Object> newListWithNullElement() {
        final List<Object> objects = new ArrayList<>();
        objects.add(null);
        return objects;
    }

    @ParameterizedTest
    @MethodSource("generateConnectorNames")
    void shouldExtractConnectorName(final List<Object> input, final String expectedName) {
        assertThat(ConnectorNameExtractor.extractConnectorName(input)).isEqualTo(expectedName);
    }

    @ParameterizedTest
    @MethodSource("generateInvalidConnectorNames")
    void shouldThrow(final Object input, final String expectedMessage) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConnectorNameExtractor.extractConnectorName(input))
                .withMessage(expectedMessage);
    }

}
