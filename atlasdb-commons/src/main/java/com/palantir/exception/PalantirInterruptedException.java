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
package com.palantir.exception;

import com.palantir.common.base.Throwables;
import com.palantir.common.exception.PalantirRuntimeException;

/**
 * The contract for {@link InterruptedException} states that if you don't handle the interrupt you
 * should either drop the interrupt flag on the thread or rethrow an {@link InterruptedException}.
 * If a {@link PalantirInterruptedException} is thrown instead of an {@link InterruptedException}
 * we should leave the interrupt flag set on the current thread to obey this contract.
 * <p>
 * This class should not be used directly, but is meant to be wrapped by
 * {@link Throwables#throwUncheckedException(Throwable)}
 */
public class PalantirInterruptedException extends PalantirRuntimeException {
    private static final long serialVersionUID = 1L;

    public PalantirInterruptedException(Throwable n) {
        super(n);
    }

    public PalantirInterruptedException(String msg) {
        super(msg);
    }

    public PalantirInterruptedException(String msg, Throwable n) {
        super(msg, n);
    }
}
