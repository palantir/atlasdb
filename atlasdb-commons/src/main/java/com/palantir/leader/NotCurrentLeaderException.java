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
package com.palantir.leader;

import com.google.common.net.HostAndPort;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.logsafe.Arg;

/**
 * Some operations may only complete if they are run on the leader.  If a non-leader server gets one of these
 * requests then this exception may the thrown to indicate that this is the case.
 *
 * @author carrino
 */
public class NotCurrentLeaderException extends ServiceNotAvailableException {
    private static final long serialVersionUID = 1L;

    public NotCurrentLeaderException(
            @CompileTimeConstant String message, Throwable cause, HostAndPort leaderHint, Arg<?>... args) {
        super(message, cause, leaderHint, args);
    }

    public NotCurrentLeaderException(@CompileTimeConstant String message, HostAndPort leaderHint, Arg<?>... args) {
        super(message, leaderHint, args);
    }

    public NotCurrentLeaderException(@CompileTimeConstant String message, Throwable cause, Arg<?>... args) {
        super(message, cause, args);
    }

    public NotCurrentLeaderException(@CompileTimeConstant String message, Arg<?>... args) {
        super(message, args);
    }
}
