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

package com.palantir.atlasdb.keyvalue.impl;

import java.util.Collection;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Stopwatch;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.logging.KvsProfilingLogger.LoggingFunction;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.base.ClosableIterator;

public class ProfilingIterator<T> implements ClosableIterator<T> {
    private final ClosableIterator<T> delegate;
    private final String kvsMethod;
    private final TableReference tableRef;
    private final RangeRequest range;

    private ProfilingIterator(ClosableIterator<T> delegate,
            String kvsMethod, TableReference tableRef, RangeRequest range) {
        this.delegate = delegate;
        this.kvsMethod = kvsMethod;
        this.tableRef = tableRef;
        this.range = range;
    }

    public static <T> ClosableIterator<T> wrap(ClosableIterator<T> iterator,
            String kvsMethod, TableReference tableRef, RangeRequest range) {
        return new ProfilingIterator<>(iterator, kvsMethod, tableRef, range);
    }

    private BiConsumer<LoggingFunction, Stopwatch> logMethodAndTimeAndTableRange(
            String method) {
        return (logger, stopwatch) ->
                logger.log("Call to iterator method {} took {} ms. This call is a result of a previous call to KVS.{} "
                        + "on table {} with range {}.",
                        LoggingArgs.method(method),
                        LoggingArgs.durationMillis(stopwatch),
                        LoggingArgs.method(kvsMethod),
                        LoggingArgs.tableRef(tableRef),
                        LoggingArgs.range(tableRef, range));
    }

    @Override
    public boolean hasNext() {
        return KvsProfilingLogger.maybeLog(() -> delegate.hasNext(), logMethodAndTimeAndTableRange("hasNext"));
    }

    @Override
    public T next() {
        return KvsProfilingLogger.maybeLog(() -> delegate.next(), logMethodAndTimeAndTableRange("next"));
    }

    @Override
    public void remove() {
        KvsProfilingLogger.maybeLog(() -> delegate.remove(), logMethodAndTimeAndTableRange("remove"));
    }

    @Override
    public void forEachRemaining(Consumer<? super T> action) {
        KvsProfilingLogger.maybeLog(() -> delegate.forEachRemaining(action),
                logMethodAndTimeAndTableRange("forEachRemaining"));
    }

    @Override
    public void close() {
        KvsProfilingLogger.maybeLog(() -> delegate.close(), logMethodAndTimeAndTableRange("close"));
    }

    @Override
    public <U> ClosableIterator<U> map(Function<T, U> mapper) {
        return KvsProfilingLogger.maybeLog(() -> ProfilingIterator.wrap(
                delegate.map(mapper), kvsMethod, tableRef, range), logMethodAndTimeAndTableRange("map"));
    }

    @Override
    public <U> ClosableIterator<U> flatMap(Function<T, Collection<U>> mapper) {
        return KvsProfilingLogger.maybeLog(() -> ProfilingIterator.wrap(
                delegate.flatMap(mapper), kvsMethod, tableRef, range), logMethodAndTimeAndTableRange("flatMap"));
    }

    @Override
    public ClosableIterator<T> stopWhen(Predicate<T> shouldStop) {
        return KvsProfilingLogger.maybeLog(() -> ProfilingIterator.wrap(
                delegate.stopWhen(shouldStop), kvsMethod, tableRef, range), logMethodAndTimeAndTableRange("stopWhen"));
    }

    @Override
    public Stream<T> stream() {
        return KvsProfilingLogger.maybeLog(() -> StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(this, 0), false), logMethodAndTimeAndTableRange("stream"));
    }
}
