/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.SweepStrategy;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public enum ThrowingSweepStrategyManager implements SweepStrategyManager {
    INSTANCE;

    @Override
    public SweepStrategy get(TableReference tableRef) {
        throw new SafeIllegalStateException(
                "This should not be called - please use a proper SweepStrategyManager if your test requires it.");
    }
}
