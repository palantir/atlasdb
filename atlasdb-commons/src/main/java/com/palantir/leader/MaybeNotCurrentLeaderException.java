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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeLoggable;
import java.util.List;

/**
 * Some operations may fail due to contention, which means we may have lost leadership.
 * In these cases, we should then validate that we have lost leadership, before taking any action.
 */
public final class MaybeNotCurrentLeaderException extends NotCurrentLeaderException implements SafeLoggable {

    private final String message;
    private final ImmutableList<Arg<?>> args;

    public MaybeNotCurrentLeaderException(@CompileTimeConstant String message, List<Arg<?>> args) {
        super(message);
        this.message = message;
        this.args = ImmutableList.copyOf(args);
    }

    public MaybeNotCurrentLeaderException(@CompileTimeConstant String message, Arg<?>... args) {
        super(message);
        this.message = message;
        this.args = ImmutableList.copyOf(args);
    }

    @Override
    public @Safe String getLogMessage() {
        return message;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }
}
