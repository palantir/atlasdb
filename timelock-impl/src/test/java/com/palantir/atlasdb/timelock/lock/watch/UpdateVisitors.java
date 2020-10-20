/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock.watch;

import static org.assertj.core.api.Assertions.fail;

import com.palantir.lock.watch.LockWatchStateUpdate;

public final class UpdateVisitors {
    private static final AssertSuccessVisitor ASSERT_SUCCESS = new AssertSuccessVisitor();
    private static final AssertSnapshotVisitor ASSERT_SNAPSHOT = new AssertSnapshotVisitor();

    private UpdateVisitors() {
        // some people...
    }

    public static LockWatchStateUpdate.Success assertSuccess(LockWatchStateUpdate update) {
        return update.accept(ASSERT_SUCCESS);
    }

    public static LockWatchStateUpdate.Snapshot assertSnapshot(LockWatchStateUpdate update) {
        return update.accept(ASSERT_SNAPSHOT);
    }

    private static final class AssertSuccessVisitor
            implements LockWatchStateUpdate.Visitor<LockWatchStateUpdate.Success> {

        @Override
        public LockWatchStateUpdate.Success visit(LockWatchStateUpdate.Success success) {
            return success;
        }

        @Override
        public LockWatchStateUpdate.Success visit(LockWatchStateUpdate.Snapshot snapshot) {
            return fail("Unexpected snapshot");
        }
    }

    public static class AssertSnapshotVisitor implements LockWatchStateUpdate.Visitor<LockWatchStateUpdate.Snapshot> {

        @Override
        public LockWatchStateUpdate.Snapshot visit(LockWatchStateUpdate.Success success) {
            return fail("Unexpected success");
        }

        @Override
        public LockWatchStateUpdate.Snapshot visit(LockWatchStateUpdate.Snapshot snapshot) {
            return snapshot;
        }
    }
}
