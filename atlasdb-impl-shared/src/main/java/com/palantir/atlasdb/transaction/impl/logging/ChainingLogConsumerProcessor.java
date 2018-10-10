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
package com.palantir.atlasdb.transaction.impl.logging;

import java.util.List;
import java.util.function.Supplier;

import org.immutables.value.Value;

import com.google.common.base.Suppliers;

/**
 * Invokes the list of {@link ChainingLogConsumerProcessor#processors()} in order.
 * Guaranteed to invoke the provided {@link Supplier&lt;LogTemplate&gt;} not more than once, even if processors
 * call get() on the Supplier multiple times.
 *
 * Behaviour is not defined if any of the processors throw.
 */
@Value.Immutable
public abstract class ChainingLogConsumerProcessor implements LogConsumerProcessor {
    protected abstract List<LogConsumerProcessor> processors();

    @Override
    public void maybeLog(Supplier<LogTemplate> logTemplateSupplier) {
        Supplier<LogTemplate> memoizingSupplier = Suppliers.memoize(logTemplateSupplier::get);
        processors().forEach(processor -> processor.maybeLog(memoizingSupplier));
    }
}
