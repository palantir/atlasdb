/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.cleaner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

class ImmutableTimestampProgressMonitorRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ImmutableTimestampProgressMonitorRunner.class);

    private final Supplier<Long> supplier;
    private final Runnable runIfFailed;
    private Long lastLockedVersionId;

    private ImmutableTimestampProgressMonitorRunner(Supplier<Long> supplier, Long lastLockedVersionId, Runnable runIfFailed){
        this.supplier = supplier;
        this.lastLockedVersionId = lastLockedVersionId;
        this.runIfFailed = runIfFailed;
    }

    static ImmutableTimestampProgressMonitorRunner of(Supplier<Long> supplier, Runnable runIfFailed) {
        return new ImmutableTimestampProgressMonitorRunner(supplier, supplier.get(), runIfFailed);
    }

    @Override
    public synchronized void run() {
        try {
            Long newLockedVersionId = supplier.get();
            if (lastLockedVersionId.equals(newLockedVersionId)) {
                runIfFailed.run();
            }
            lastLockedVersionId = newLockedVersionId;
        } catch (Exception e) {
            log.error("Checking immutable timestamp failed", e);
        }
    }
}
