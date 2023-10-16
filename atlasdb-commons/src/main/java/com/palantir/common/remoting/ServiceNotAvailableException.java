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
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * If a server is shutting down or cannot respond to a call for another reason this exception may be thrown.
 * This exception indicates to the caller that this call should be retried on another server that is available.
 *
 * @author carrino
 */
public class ServiceNotAvailableException extends RuntimeException implements SafeLoggable {
    private static final long serialVersionUID = 2L;

    private final Optional<HostAndPort> serviceHint;
    private final List<Arg<?>> args;

    public ServiceNotAvailableException(
            @CompileTimeConstant String message, Throwable cause, HostAndPort serviceHint, Arg<?>... args) {
        super(message, cause);
        this.serviceHint = Optional.of(serviceHint);
        this.args = toArgListWithSafeServiceHint(args);
    }

    public ServiceNotAvailableException(@CompileTimeConstant String message, HostAndPort serviceHint, Arg<?>... args) {
        super(message);
        this.serviceHint = Optional.of(serviceHint);
        this.args = toArgListWithSafeServiceHint(args);
    }

    public ServiceNotAvailableException(@CompileTimeConstant String message, Throwable cause, Arg<?>... args) {
        super(message, cause);
        this.serviceHint = Optional.empty();
        this.args = toArgListWithSafeServiceHint(args);
    }

    public ServiceNotAvailableException(@CompileTimeConstant String message, Arg<?>... args) {
        super(message);
        this.serviceHint = Optional.empty();
        this.args = toArgListWithSafeServiceHint(args);
    }

    public ServiceNotAvailableException(Throwable cause, Arg<?>... args) {
        super(cause);
        this.serviceHint = Optional.empty();
        this.args = toArgListWithSafeServiceHint(args);
    }

    public Optional<HostAndPort> getServiceHint() {
        return serviceHint;
    }

    @Override
    public @Safe String getLogMessage() {
        return Objects.requireNonNullElse(getMessage(), "[No message specified]");
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }

    private List<Arg<?>> toArgListWithSafeServiceHint(Arg<?>[] args) {
        return ImmutableList.<Arg<?>>builderWithExpectedSize(args.length + 1)
                .add(args)
                .add(SafeArg.of("serviceHint", serviceHint))
                .build();
    }
}
