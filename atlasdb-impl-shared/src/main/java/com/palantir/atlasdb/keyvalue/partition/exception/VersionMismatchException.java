/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.partition.exception;

import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;

public abstract class VersionMismatchException extends TransactionFailedRetriableException {

    private static final long serialVersionUID = 5540601852684261371L;

    public VersionMismatchException(String message) {
        super(message);
    }

    public VersionMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

}
