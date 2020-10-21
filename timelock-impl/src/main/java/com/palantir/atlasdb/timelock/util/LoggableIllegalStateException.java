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
package com.palantir.atlasdb.timelock.util;

import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeLoggable;
import java.util.List;

// TODO(nziebart): replace this when http-remoting provides a similar class
public class LoggableIllegalStateException extends IllegalStateException implements SafeLoggable {

    private final List<Arg<?>> args;

    public LoggableIllegalStateException(String message, Arg<?>... args) {
        super(message);

        this.args = ImmutableList.copyOf(args);
    }

    @Override
    public String getLogMessage() {
        return getMessage();
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }
}
