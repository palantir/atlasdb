/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.util;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeLoggable;

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
