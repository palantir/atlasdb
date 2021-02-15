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

import com.google.common.net.HostAndPort;
import java.util.Optional;

/**
 * If a server is shutting down or cannot respond to a call for another reson this exception may be thrown.
 * This exception indicates to the caller that this call should be retried on another server that is available.
 *
 * @author carrino
 */
public class ServiceNotAvailableException extends RuntimeException {
    private static final long serialVersionUID = 2L;

    private final Optional<HostAndPort> serviceHint;

    public ServiceNotAvailableException(String message, Throwable cause, HostAndPort serviceHint) {
        super(message, cause);
        this.serviceHint = Optional.of(serviceHint);
    }

    public ServiceNotAvailableException(String message, HostAndPort serviceHint) {
        super(message);
        this.serviceHint = Optional.of(serviceHint);
    }

    public ServiceNotAvailableException(String message, Throwable cause) {
        super(message, cause);
        this.serviceHint = Optional.empty();
    }

    public ServiceNotAvailableException(String message) {
        super(message);
        this.serviceHint = Optional.empty();
    }

    public ServiceNotAvailableException(Throwable cause) {
        super(cause);
        this.serviceHint = Optional.empty();
    }

    public Optional<HostAndPort> getServiceHint() {
        return serviceHint;
    }
}
