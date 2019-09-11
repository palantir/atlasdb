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

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import com.datastax.driver.core.Row;

public final class RowStreamAccumulators {

    private RowStreamAccumulators() {

    }

    public static class CurrentTimeAccumulator implements RowStreamAccumulator<String> {

        private final AtomicReference<String> resultReference = new AtomicReference<>();

        public CurrentTimeAccumulator() {

        }

        @Override
        public void accumulateRowStream(Stream<Row> rowStream) {
            rowStream.forEach(this::processRow);
        }

        @Override
        public String result() {
            if (resultReference.get() == null) {
                throw new RuntimeException("Processing current cluster time query raises an exception");
            }
            return resultReference.get();
        }

        private void processRow(Row row) {
            resultReference.compareAndSet(null, row.getTimestamp(0).toString());
        }
    }
}
