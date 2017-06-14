/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.tracing;

import javax.annotation.Nullable;

import org.slf4j.helpers.MessageFormatter;

import com.google.common.collect.ObjectArrays;
import com.palantir.remoting2.tracing.OpenSpan;
import com.palantir.remoting2.tracing.SpanType;
import com.palantir.remoting2.tracing.Tracer;

public final class CloseableTrace implements AutoCloseable {

    private static final CloseableTrace NO_OP = new CloseableTrace(null);

    private final OpenSpan trace;

    private CloseableTrace(@Nullable OpenSpan trace) {
        this.trace = trace;
    }

    @Override
    public void close() {
        if (trace != null) {
            Tracer.completeSpan();
        }
    }

    public static CloseableTrace noOp() {
        return NO_OP;
    }

    public static CloseableTrace startLocalTrace(String serviceName,
            CharSequence operationFormat,
            Object... formatArguments) {
        if (Tracer.isTraceObservable()) {
            return startLocalTrace("{}." + operationFormat,
                    ObjectArrays.concat(serviceName, formatArguments));
        }
        return noOp();
    }

    private static CloseableTrace startLocalTrace(CharSequence operationFormat, Object... formatArguments) {
        if (Tracer.isTraceObservable()) {
            String operation = MessageFormatter.arrayFormat(operationFormat.toString(), formatArguments).getMessage();
            OpenSpan openSpan = Tracer.startSpan(operation, SpanType.LOCAL);
            return new CloseableTrace(openSpan);
        }
        return noOp();
    }

}
