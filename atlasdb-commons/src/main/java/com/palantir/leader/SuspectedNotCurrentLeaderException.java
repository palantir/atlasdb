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
 * This exception may additionally provide a callback that an external environment can use as a mechanism for feedback
 * to the original class, that leadership was indeed lost. This is slightly horrible, but the creation path of the
 * objects that might produce this exception is sufficiently complex that the architecturally preferable solution of
 * passing the leadership coordinator or mechanism for checking if we still have leadership to the relevant objects is
 * not feasible without invasive refactoring, which we don't have the resources for right now.
 * <p>
 * A precondition of this callback is that it does not throw exceptions. If it does, the behaviour of the system is
 * undefined.
 * <p>
 * If leadership was not actually lost,
 * Throwers which require availability must not throw this exception when they are in an irrecoverable state, even if
 * the source of that state is only a possible leadership loss. They should instead consider throwing
 * {@link NotCurrentLeaderException}.
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
