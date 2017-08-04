/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.deepkin;

import java.util.Comparator;
import java.util.Objects;

import com.google.common.collect.TreeMultimap;
import com.palantir.remoting2.tracing.Span;

public class OrderedTraceCalls {
    private final String traceId;
    private final TreeMultimap<String, MethodCall> methodCalls;

    private OrderedTraceCalls(String traceId) {
        this.traceId = traceId;
        this.methodCalls = TreeMultimap.create(String::compareTo, Comparator.comparingLong(MethodCall::callTime));
    }

    public MethodCall pop(String name) {
        return methodCalls.get(name).pollFirst();
    }

    public boolean isEmpty() {
        return methodCalls.isEmpty();
    }

    public String getTraceId() {
        return traceId;
    }

    public static OrderedTraceCalls fromSpans(Iterable<Span> spans) {
        OrderedTraceCalls calls = null;
        for (Span span : spans) {
            if (calls == null) {
                calls = new OrderedTraceCalls(span.getTraceId());
            } else if (!Objects.equals(calls.traceId, span.getTraceId())) {
                throw new IllegalStateException("OrderedTraceCalls must only contain calls for a single trace");
            }
            calls.methodCalls.put(span.getOperation(), MethodCall.fromSpan(span));
        }
        return calls;
    }
}
