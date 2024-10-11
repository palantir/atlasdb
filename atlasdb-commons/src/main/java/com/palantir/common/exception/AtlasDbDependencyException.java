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

import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.exceptions.SafeExceptions;
import java.util.List;
import javax.annotation.Nullable;

public class AtlasDbDependencyException extends RuntimeException implements SafeLoggable {
    private static final String LOG_MESSAGE = "AtlasDB dependency threw an exception.";
    private List<Arg<?>> args = List.of();
    private String logMessage = "";

    public AtlasDbDependencyException(String logMessage) {
        super(logMessage);
    }

    public AtlasDbDependencyException(String logMessage, Arg<?>... args) {
        this(logMessage, List.of(args));
    }

    public AtlasDbDependencyException(String logMessage, Throwable cause) {
        super(SafeExceptions.renderMessage(logMessage), cause);
        this.logMessage = logMessage;
    }

    public AtlasDbDependencyException(Throwable throwable) {
        super(LOG_MESSAGE, throwable);
    }

    public AtlasDbDependencyException(Throwable throwable, Arg<?>... args) {
        this(throwable, List.of(args));
    }

    private AtlasDbDependencyException(@Nullable Throwable cause, List<Arg<?>> args) {
        super(SafeExceptions.renderMessage(LOG_MESSAGE, args.toArray(new Arg[0])), cause);
        this.args = args;
        this.logMessage = LOG_MESSAGE;
    }

    private AtlasDbDependencyException(String logMessage, List<Arg<?>> args) {
        super(SafeExceptions.renderMessage(logMessage, args.toArray(new Arg[0])));
        this.args = args;
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
