/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api.exceptions;

import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.exceptions.SafeExceptions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

public class SafeTransactionFailedRetriableException extends TransactionFailedRetriableException
        implements SafeLoggable {
    private final String logMessage;
    private final List<Arg<?>> arguments;

    public SafeTransactionFailedRetriableException(@CompileTimeConstant String message, Arg<?>... arguments) {
        super(SafeExceptions.renderMessage(message, arguments));
        this.logMessage = message;
        this.arguments = Collections.unmodifiableList(Arrays.asList(arguments));
    }

    public SafeTransactionFailedRetriableException(
            @CompileTimeConstant String message, @Nullable Throwable cause, Arg<?>... arguments) {
        super(SafeExceptions.renderMessage(message, arguments), cause);
        this.logMessage = message;
        this.arguments = Collections.unmodifiableList(Arrays.asList(arguments));
    }

    @Override
    public String getLogMessage() {
        return logMessage;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return arguments;
    }
}
