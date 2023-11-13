/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.leader;

import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.exceptions.SafeExceptions;
import java.util.List;

/**
 * This exception may be thrown when an operation that indicates a possible leadership loss has occurred. This does
 * not mean that leadership has been lost; the handler should check the leadership status and act accordingly.
 * <p>
 * If leadership was not actually lost, the underlying object may be in an inconsistent state, and will be recreated
 * before further requests are sent to it.
 */
public class SuspectedNotCurrentLeaderException extends RuntimeException implements SafeLoggable {
    private final String logMessage;
    private final List<Arg<?>> args;

    public SuspectedNotCurrentLeaderException(@CompileTimeConstant String logMessage, Arg<?>... args) {
        super(SafeExceptions.renderMessage(logMessage, args));
        this.logMessage = logMessage;
        this.args = List.of(args);
    }

    @Override
    public @Safe String getLogMessage() {
        return logMessage;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }
}
