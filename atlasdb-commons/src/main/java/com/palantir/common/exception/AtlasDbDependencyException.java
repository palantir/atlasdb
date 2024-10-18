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
package com.palantir.common.exception;

import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.exceptions.SafeExceptions;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

public class AtlasDbDependencyException extends RuntimeException implements SafeLoggable {
    private final List<Arg<?>> args;
    private final String logMessage;

    public AtlasDbDependencyException(@CompileTimeConstant String logMessage, Arg<?>... args) {
        this(logMessage, null, args);
    }

    public AtlasDbDependencyException(Throwable throwable, Arg<?>... args) {
        this("AtlasDB dependency threw an exception.", throwable, args);
    }

    public AtlasDbDependencyException(
            @Safe @CompileTimeConstant String logMessage, @Nullable Throwable cause, Arg<?>... args) {
        super(SafeExceptions.renderMessage(logMessage, args), cause);
        this.args = Arrays.asList(args);
        this.logMessage = logMessage;
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
