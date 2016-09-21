/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.util;

final class SuppressedException extends RuntimeException {
    static final long serialVersionUID = 1L;

    SuppressedException(String message, Throwable cause) {
        super(message, cause);
    }

    public static Throwable from(Throwable throwable) {
        String type = (throwable instanceof Error) ? "Error"
                : (throwable instanceof RuntimeException) ? "RuntimeException"
                        : (throwable instanceof Exception) ? "Exception"
                                : "Throwable";
        String message = String.format("%s [%s] occurred while processing thread (%s)",
                type, throwable, Thread.currentThread().getName());
        return new SuppressedException(message, throwable);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return null;
    }

    @Override
    public String toString() {
        String message = getLocalizedMessage();
        return (message == null) ? getClass().getSimpleName() : message;
    }
}
