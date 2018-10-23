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
package com.palantir.atlasdb.transaction.api;

/**
 * This is a generic class of failures that could happen during a transaction where we can retry the
 * transaction. This may be a simple failure to do a {@link Transaction#get} or a write.
 *
 * @author carrino
 *
 */
public class TransactionFailedRetriableException extends TransactionFailedException {
    private static final long serialVersionUID = 1L;

    public TransactionFailedRetriableException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransactionFailedRetriableException(String message) {
        super(message);
    }

    @Override
    public final boolean canTransactionBeRetried() {
        return true;
    }
}
