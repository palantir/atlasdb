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

package com.palantir.atlasdb.logging;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.util.Optional;
import java.util.function.Function;

public final class DefaultSensitiveLoggingArgProducers {
    public static final SensitiveLoggingArgProducer ALWAYS_UNSAFE = new DefaultSensitiveLoggingArgProducer(false);
    public static final SensitiveLoggingArgProducer ALWAYS_SAFE = new DefaultSensitiveLoggingArgProducer(true);

    private DefaultSensitiveLoggingArgProducers() {
        // nope
    }

    private static class DefaultSensitiveLoggingArgProducer implements SensitiveLoggingArgProducer {
        private final boolean safe;

        private DefaultSensitiveLoggingArgProducer(boolean safe) {
            this.safe = safe;
        }

        private Arg<?> getArg(String name, byte[] intendedValue, Function<byte[], Object> transform) {
            return safe
                    ? SafeArg.of(name, transform.apply(intendedValue))
                    : UnsafeArg.of(name, transform.apply(intendedValue));
        }

        @Override
        public Optional<Arg<?>> getArgForRow(
                TableReference tableReference, byte[] row, Function<byte[], Object> transform) {
            return Optional.of(getArg("row", row, transform));
        }

        @Override
        public Optional<Arg<?>> getArgForDynamicColumnsColumnKey(
                TableReference tableReference, byte[] row, Function<byte[], Object> transform) {
            return Optional.of(getArg("columnKey", row, transform));
        }

        @Override
        public Optional<Arg<?>> getArgForValue(
                TableReference tableReference, Cell cellReference, byte[] value, Function<byte[], Object> transform) {
            return Optional.of(getArg("value", value, transform));
        }
    }
}
