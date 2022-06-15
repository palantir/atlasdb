/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.tracing;

import java.util.concurrent.atomic.AtomicLong;

class TraceStatisticImpl implements TraceStatistic {
    private final AtomicLong emptyReads;

    private TraceStatisticImpl(long emptyReads) {
        this.emptyReads = new AtomicLong(emptyReads);
    }

    @Override
    public boolean isEmpty() {
        return emptyReads.get() == 0;
    }

    @Override
    public TraceStatistic copy() {
        return of(emptyReads());
    }

    @Override
    public long emptyReads() {
        return emptyReads.get();
    }

    @Override
    public void incEmptyReads(long count) {
        emptyReads.addAndGet(count);
    }
}
