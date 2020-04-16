/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.v2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class StartIdentifiedAtlasDbTransactionResponseBatch implements AutoCloseable {

    private final List<StartIdentifiedAtlasDbTransactionResponse> responses;
    private final Consumer<StartIdentifiedAtlasDbTransactionResponse> cleaner;
    private boolean closed;
    private final long minTimestamp;
    private final long maxTimestamp;

    private StartIdentifiedAtlasDbTransactionResponseBatch(List<StartIdentifiedAtlasDbTransactionResponse> responses,
            Consumer<StartIdentifiedAtlasDbTransactionResponse> cleaner) {
        this.responses = responses;
        this.cleaner = cleaner;
        this.closed = false;
        List<Long> timestamps = responses
                .stream()
                .map(response -> response.immutableTimestamp().getImmutableTimestamp())
                .collect(Collectors.toList());
        this.minTimestamp = Collections.min(timestamps);
        this.maxTimestamp = Collections.max(timestamps);
    }

    public int size() {
        return responses.size();
    }

    public List<StartIdentifiedAtlasDbTransactionResponse> getResponses() {
        return responses;
    }

    public <R> R successful(R value) {
        closed = true;
        return value;
    }

    @Override
    public void close() {
        if (!closed) {
            try (ExceptionHandlingRunner closer = new ExceptionHandlingRunner()) {
                responses.forEach(resource -> closer.runSafely(() -> cleaner.accept(resource)));
            }
        }
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public static class Builder implements AutoCloseable {
        private final List<StartIdentifiedAtlasDbTransactionResponse> responses = new ArrayList<>();
        private final ExceptionHandlingRunner runner = new ExceptionHandlingRunner();
        private final Consumer<StartIdentifiedAtlasDbTransactionResponse> cleaner;

        public Builder(Consumer<StartIdentifiedAtlasDbTransactionResponse> cleaner) {
            this.cleaner = cleaner;
        }

        public StartIdentifiedAtlasDbTransactionResponse safeAddToBatch(
                Supplier<StartIdentifiedAtlasDbTransactionResponse> supplier) {
            StartIdentifiedAtlasDbTransactionResponse response = runner.supplySafely(supplier);
            if (response != null) {
                responses.add(response);
            }
            return response;
        }

        public StartIdentifiedAtlasDbTransactionResponseBatch build() {
            return new StartIdentifiedAtlasDbTransactionResponseBatch(responses, cleaner);
        }

        @Override
        public void close() {
            try {
                runner.close();
            } catch (Throwable t) {
                try (ExceptionHandlingRunner closer = new ExceptionHandlingRunner(t)) {
                    responses.forEach(resource -> closer.runSafely(() -> cleaner.accept(resource)));
                }
            }
        }
    }
}
