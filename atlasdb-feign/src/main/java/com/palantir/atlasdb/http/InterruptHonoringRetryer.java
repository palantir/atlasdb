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

package com.palantir.atlasdb.http;

import java.io.InterruptedIOException;

import feign.RetryableException;
import feign.Retryer;

/**
 * Simple wrapper around the default Feign retryer that propagates {@link InterruptedIOException}s
 * rather than retrying them. This allows the request thread to be interrupted.
 */
public class InterruptHonoringRetryer implements Retryer {

    private final Retryer delegate = new Retryer.Default();

    @Override
    public void continueOrPropagate(RetryableException error) {
        if (error.getCause() instanceof InterruptedIOException) {
            throw new RuntimeException(error.getCause());
        }

        delegate.continueOrPropagate(error);
    }

    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    @Override
    public Retryer clone() {
        return new InterruptHonoringRetryer();
    }
}
