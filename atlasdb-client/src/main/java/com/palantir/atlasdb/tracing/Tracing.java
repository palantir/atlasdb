/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.google.errorprone.annotations.CompileTimeConstant;
import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.tracing.CloseableTracer;
import java.util.function.Consumer;

public interface Tracing {

    @MustBeClosed
    static AtlasCloseableTracer startLocalStatAwareSpan(
            @CompileTimeConstant final String operation, Consumer<TagConsumer> tagTranslator) {
        TraceStatistic oldValue = TraceStatistics.clearAndGetOld();

        return new TraceClosingAtlasCloseableTracer(
                oldValue,
                CloseableTracer.startSpan(
                        operation, FunctionalTagTranslator.INSTANCE, new TraceAddingTagConsumer(tagTranslator)));
    }

    @MustBeClosed
    static CloseableTracer startLocalSpan(
            @CompileTimeConstant final String operation, Consumer<TagConsumer> tagTranslator) {
        return CloseableTracer.startSpan(operation, FunctionalTagTranslator.INSTANCE, tagTranslator);
    }
}
