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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.remoting2.tracing.Span;
import com.palantir.remoting2.tracing.Tracer;

import okio.ByteString;

public class TimeOrderedReplayer implements ReplayerService {
    private final Map<String, OrderedTraceCalls> traces = Maps.newConcurrentMap();

    @Override
    public <Y, T> T getResult(TransactionMethod<Y, T> method, Object... arguments) {
        long start = System.currentTimeMillis();

        OrderedTraceCalls calls = traces.get(Tracer.getTraceId());
        Preconditions.checkNotNull(calls, "Unable to get calls for traceId");

        MethodCall call = calls.pop(method.name());
        Preconditions.checkNotNull(call, "Unable to get call for method {}", method.name());

        if (calls.isEmpty()) {
            traces.remove(calls.getTraceId());
        }

        ResultTransform<Y, T> resultTransform = method.resultTransform();
        //TODO(@cbh) Typecheck against resultTransform.serializeType
        Object ser = unsafeLoad(call.response());

        try {
            Thread.sleep(Math.max(0, (start - System.currentTimeMillis() + (call.responseTime() / 1000 / 1000))));
        } catch (InterruptedException exception) {
            throw new IllegalStateException("Interrupted thread");
        }
        return resultTransform.deserializer().apply((Y) ser);
    }

    private Object unsafeLoad(ByteString bytes) {
        try {
            return new ObjectInputStream(new ByteBufferBackedInputStream(bytes.asByteBuffer())).readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Unable to read object from ByteString");
        }
    }

    protected void addTrace(List<Span> spans) {
        OrderedTraceCalls calls = OrderedTraceCalls.fromSpans(spans);
        traces.put(calls.getTraceId(), calls);
    }
}
