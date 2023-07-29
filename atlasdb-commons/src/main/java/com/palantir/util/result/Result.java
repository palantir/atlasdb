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

package com.palantir.util.result;

import com.palantir.logsafe.exceptions.SafeRuntimeException;

public abstract class Result<T, E> {

    private Result() {}

    public static final class Ok<T, E> extends Result<T, E> {
        private final T value;

        public Ok(T value) {
            this.value = value;
        }

        public T getValue() {
            return value;
        }
    }

    public static final class Err<T, E> extends Result<T, E> {
        private final E error;

        public Err(E error) {
            this.error = error;
        }

        public E getError() {
            return error;
        }
    }

    public final boolean isOk() {
        return this instanceof Ok;
    }

    public final boolean isErr() {
        return this instanceof Err;
    }

    public T unwrap() {
        if (this instanceof Ok) {
            return ((Ok<T, E>) this).getValue();
        } else {
            throw new SafeRuntimeException("Called unwrap() on an Err value");
        }
    }

    public E unwrapErr() {
        if (this instanceof Err) {
            return ((Err<T, E>) this).getError();
        } else {
            throw new SafeRuntimeException("Called unwrapErr() on an Ok value");
        }
    }
}
