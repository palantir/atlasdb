/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.lock;

/**
 * A BlockingTimeoutException is used to indicate that a lock request has blocked for too long, and that
 * the server will no longer service the request. The request may be retried.
 */
public class BlockingTimeoutException extends RuntimeException {
    public BlockingTimeoutException(Throwable cause) {
        super(cause);
    }
}
