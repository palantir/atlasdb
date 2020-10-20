/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.tracing;

import com.google.common.collect.ObjectArrays;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.SpanType;
import javax.annotation.Nullable;
import org.slf4j.helpers.MessageFormatter;

public final class CloseableTrace implements AutoCloseable {

    private static final CloseableTrace NO_OP = new CloseableTrace(null);

    private final OpenSpan trace;

    private CloseableTrace(@Nullable OpenSpan trace) {
        this.trace = trace;
    }

    @Override
    public void close() {
        if (trace != null) {
            Tracer.fastCompleteSpan();
        }
    }

    public static CloseableTrace noOp() {
        return NO_OP;
    }

    public static CloseableTrace startLocalTrace(
            String serviceName, CharSequence operationFormat, Object... formatArguments) {
        if (Tracer.isTraceObservable()) {
            return startLocalTrace("{}." + operationFormat, ObjectArrays.concat(serviceName, formatArguments));
        }
        return noOp();
    }

    private static CloseableTrace startLocalTrace(CharSequence operationFormat, Object... formatArguments) {
        if (Tracer.isTraceObservable()) {
            String operation = MessageFormatter.arrayFormat(operationFormat.toString(), formatArguments)
                    .getMessage();
            OpenSpan openSpan = Tracer.startSpan(operation, SpanType.LOCAL);
            return new CloseableTrace(openSpan);
        }
        return noOp();
    }
}
