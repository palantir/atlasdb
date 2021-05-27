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
package com.palantir.atlasdb.transaction.impl;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StdoutTracingTransaction extends TracingTransaction {
    public StdoutTracingTransaction(Transaction delegate) {
        super(delegate);
    }

    @Override
    protected boolean isTraceEnabled() {
        return true;
    }

    @Override
    @FormatMethod
    protected void trace(@FormatString String format, Object... args) {
        System.out.println(String.format(format, args)); // (authorized)
    }
}
