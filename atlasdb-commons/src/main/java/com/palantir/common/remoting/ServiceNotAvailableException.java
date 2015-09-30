/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.common.remoting;

/**
 * If a server is shutting down or cannot respond to a call for another reson this exception may be thrown.
 * This exception indicates to the caller that this call should be retried on another server that is available.
 *
 * @author carrino
 */
public class ServiceNotAvailableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ServiceNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceNotAvailableException(String message) {
        super(message);
    }

    public ServiceNotAvailableException(Throwable cause) {
        super(cause);
    }
}
