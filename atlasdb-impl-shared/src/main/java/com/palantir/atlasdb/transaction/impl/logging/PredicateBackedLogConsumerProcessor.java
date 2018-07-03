/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.logging;

import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.immutables.value.Value;

@Value.Immutable
public abstract class PredicateBackedLogConsumerProcessor implements LogConsumerProcessor {
    @Value.Parameter
    protected abstract BiConsumer<String, Object[]> logFunction();

    @Value.Parameter
    protected abstract BooleanSupplier predicate();

    public void maybeLog(Supplier<LogTemplate> logTemplateSupplier) {
        if (predicate().getAsBoolean()) {
            LogTemplate template = logTemplateSupplier.get();
            logFunction().accept(template.format(), template.arguments());
        }
    }

    public static LogConsumerProcessor create(BiConsumer<String, Object[]> logFunction, BooleanSupplier predicate) {
        return ImmutablePredicateBackedLogConsumerProcessor.of(logFunction, predicate);
    }
}
