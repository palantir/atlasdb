// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.exception;

import com.palantir.common.exception.PalantirRuntimeException;

/**InterruptedExceptions are now thrown out of SQL calls. Generally speaking, we just want to propagate them.
 * Having a whole bunch of 'throws' and 'catch throws' is ugly & unnecessary (since in theory, code where an exception can
 * mess up data integrity in the db should be wrapped in a transaction).
 * <p>
 * The contract for {@link InterruptedException} states that if you don't handle the interrupt you
 * should either drop the interrupt flag on the thread or rethrow an {@link InterruptedException}.
 * If a {@link PalantirInterruptedException} is thrown instead of an {@link InterruptedException}
 * we should leave the interrupt flag set on the current thread to obey this contract.
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

