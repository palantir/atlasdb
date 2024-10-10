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

import com.palantir.atlasdb.cell.api.AutoDelegate_DataKeyValueService;
import com.palantir.atlasdb.cell.api.DataKeyValueService;
import java.util.concurrent.atomic.AtomicBoolean;

public final class InvalidatableDataKeyValueService implements AutoDelegate_DataKeyValueService {
    private final DataKeyValueService delegate;
    private final AtomicBoolean isStillValid;

    public InvalidatableDataKeyValueService(DataKeyValueService delegate) {
        this.delegate = delegate;
        this.isStillValid = new AtomicBoolean(true);
    }

    @Override
    public DataKeyValueService delegate() {
        return delegate;
    }

    @Override
    public boolean isValid(long _timestamp) {
        return isStillValid.get();
    }

    public void invalidate() {
        isStillValid.set(false);
    }
}
