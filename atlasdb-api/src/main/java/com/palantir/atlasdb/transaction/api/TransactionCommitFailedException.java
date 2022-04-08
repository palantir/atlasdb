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
 * This exception type represents a failure during commit where we cannot be sure if the transaction
 * succeeded or not.  This is a {@link TransactionFailedException} that cannot be retried.
 * <p>
 * Since Transactions can be implemented as distributed transactions this failure happens as we request an
 * atomic commit.  The system that handles this request may have failed in some way and we can't verify
 * if the transaction was successful or not.
 *
 * @author carrino
 *
 */
public class TransactionCommitFailedException extends TransactionFailedNonRetriableException {
    private static final long serialVersionUID = 1L;

    public TransactionCommitFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransactionCommitFailedException(String message) {
        super(message);
    }

    @Override
    public String getLogMessage() {
        return "Transaction commit failed";
    }
}
