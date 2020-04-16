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
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class StartIdentifiedAtlasDbTransactionResponseBatch implements AutoCloseable {

    private final List<StartIdentifiedAtlasDbTransactionResponse> responses;
    private final Consumer<StartIdentifiedAtlasDbTransactionResponse> cleaner;
    private final ExceptionHandlingRunner runner = new ExceptionHandlingRunner();

    public StartIdentifiedAtlasDbTransactionResponseBatch(Consumer<StartIdentifiedAtlasDbTransactionResponse> cleaner) {
        this(new ArrayList<>(), cleaner);
    }

    private StartIdentifiedAtlasDbTransactionResponseBatch(List<StartIdentifiedAtlasDbTransactionResponse> responses,
            Consumer<StartIdentifiedAtlasDbTransactionResponse> cleaner) {
        this.responses = responses;
        this.cleaner = cleaner;
    }

    public StartIdentifiedAtlasDbTransactionResponse safeExecute(
            Supplier<StartIdentifiedAtlasDbTransactionResponse> supplier) {
        StartIdentifiedAtlasDbTransactionResponse response = runner.supplySafely(supplier);
        if (response != null) {
            responses.add(response);
        }
        return response;
    }

    public int size() {
        return responses.size();
    }

    public List<StartIdentifiedAtlasDbTransactionResponse> getResponses() {
        return responses;
    }

    public StartIdentifiedAtlasDbTransactionResponseBatch copy() {
        return new StartIdentifiedAtlasDbTransactionResponseBatch(responses, cleaner);
    }

    @Override
    public void close() {
        try {
            // If there are no errors, then this will not throw, and NO CLEANUP WILL BE DONE (which is what we want,
            // because cleanup -> unlocking things).
            runner.close();
        } catch (Throwable t) {
            // Otherwise, we now close everything. If something throws mid-way, we capture
            // and then at the end close, which may throw, propagating up (as we expect).
            try (ExceptionHandlingRunner closer = new ExceptionHandlingRunner(t)) {
                responses.forEach(resource -> closer.runSafely(() -> cleaner.accept(resource)));
            }
        }

    }
}
