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
package com.palantir.atlasdb.util;

final class SuppressedException extends RuntimeException {
    static final long serialVersionUID = 1L;
    private static final Class<?>[] namedThrowables = {Error.class, RuntimeException.class, Exception.class};

    SuppressedException(String message) {
        super(message);
    }

    public static Throwable from(Throwable throwable) {
        String message = String.format(
                "%s [%s] occurred while processing thread (%s)",
                highLevelType(throwable), throwable, Thread.currentThread().getName());
        SuppressedException suppressedException = new SuppressedException(message);
        suppressedException.setStackTrace(throwable.getStackTrace());
        return suppressedException;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return null;
    }

    @Override
    public String getMessage() {
        String message = getLocalizedMessage();
        return (message == null) ? getClass().getSimpleName() : message;
    }

    private static String highLevelType(Throwable throwable) {
        for (Class<?> namedClass : namedThrowables) {
            if (namedClass.isInstance(throwable)) {
                return namedClass.getSimpleName();
            }
        }
        return Throwable.class.getSimpleName();
    }
}
