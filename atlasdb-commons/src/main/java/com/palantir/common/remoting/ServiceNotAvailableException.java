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
package com.palantir.common.remoting;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.exceptions.SafeExceptions;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * If a server is shutting down or cannot respond to a call for another reason this exception may be thrown.
 * This exception indicates to the caller that this call should be retried on another server that is available.
 *
 * @author carrino
 */
public class ServiceNotAvailableException extends RuntimeException implements SafeLoggable {
    private static final long serialVersionUID = 2L;

    private final Optional<HostAndPort> serviceHint;
    private final String logMessage;
    private final List<Arg<?>> args;

    public ServiceNotAvailableException(
            @CompileTimeConstant String logMessage, Throwable cause, HostAndPort serviceHint, Arg<?>... args) {
        this(logMessage, cause, Optional.of(serviceHint), toArgListWithSafeServiceHint(Optional.of(serviceHint), args));
    }

    public ServiceNotAvailableException(
            @CompileTimeConstant String logMessage, HostAndPort serviceHint, Arg<?>... args) {
        this(logMessage, null, Optional.of(serviceHint), toArgListWithSafeServiceHint(Optional.of(serviceHint), args));
    }

    public ServiceNotAvailableException(@CompileTimeConstant String logMessage, Throwable cause, Arg<?>... args) {
        this(logMessage, cause, Optional.empty(), toArgListWithSafeServiceHint(Optional.empty(), args));
    }

    public ServiceNotAvailableException(@CompileTimeConstant String logMessage, Arg<?>... args) {
        this(logMessage, null, Optional.empty(), toArgListWithSafeServiceHint(Optional.empty(), args));
    }

    public ServiceNotAvailableException(Throwable cause, Arg<?>... args) {
        this("", cause, Optional.empty(), toArgListWithSafeServiceHint(Optional.empty(), args));
    }

    private ServiceNotAvailableException(
            @CompileTimeConstant String logMessage,
            @Nullable Throwable cause,
            Optional<HostAndPort> serviceHint,
            List<Arg<?>> args) {
        super(SafeExceptions.renderMessage(logMessage, args.toArray(new Arg[0])), cause);
        this.logMessage = logMessage;
        this.serviceHint = serviceHint;
        this.args = args;
    }

    public Optional<HostAndPort> getServiceHint() {
        return serviceHint;
    }

    @Override
    public @Safe String getLogMessage() {
        return logMessage;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }

    private static List<Arg<?>> toArgListWithSafeServiceHint(Optional<HostAndPort> serviceHint, Arg<?>[] args) {
        return ImmutableList.<Arg<?>>builderWithExpectedSize(args.length + 1)
                .add(args)
                .add(SafeArg.of("serviceHint", serviceHint))
                .build();
    }
}
