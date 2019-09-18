/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;

import com.datastax.driver.core.Row;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.keyvalue.api.Value;

public final class RowStreamAccumulators {

    private RowStreamAccumulators() {

    }

    @NotThreadSafe
    public static class GetQueryAccumulator implements RowStreamAccumulator<Optional<Value>> {

        private Optional<Value> resultValue = Optional.empty();

        @Override
        public void accumulateRowStream(Stream<Row> rowStream) {
            Stream<Value> valueStream = rowStream.map(GetQueryAccumulator::parseValue);
            resultValue = resultValue.map(value -> Streams.concat(Stream.of(value), valueStream))
                    .orElse(valueStream)
                    .max(Comparator.comparingLong(Value::getTimestamp));
        }

        @Override
        public Optional<Value> result() {
            return resultValue;
        }

        private static Value parseValue(Row row) {
            return Value.create(row.getBytes(0).array(), ~row.getLong(1));
        }
    }
}
