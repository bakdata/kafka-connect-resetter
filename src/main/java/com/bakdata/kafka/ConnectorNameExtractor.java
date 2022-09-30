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

import java.util.List;
import java.util.Optional;
import lombok.experimental.UtilityClass;

@UtilityClass
class ConnectorNameExtractor {
    String extractConnectorName(final Object o) {
        if (o instanceof List) {
            final List<Object> value = (List<Object>) o;
            if (value.isEmpty()) {
                throw new IllegalArgumentException("Expected at least one element");
            } else {
                final Object first = value.get(0);
                if (first instanceof String) {
                    return (String) first;
                } else {
                    throw new IllegalArgumentException(
                            "Expected first element to be string but got " + getClass(first));
                }
            }
        } else {
            throw new IllegalArgumentException("Expected record to be a list but got " + getClass(o));
        }
    }

    private String getClass(final Object o) {
        return Optional.ofNullable(o)
                .map(Object::getClass)
                .map(Class::getName)
                .orElse("null");
    }
}
