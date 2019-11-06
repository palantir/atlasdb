/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.Map;

import org.junit.Test;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.lock.LockWatchingResource;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockWatch;

public class LockWatchingEteTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");

    private final LockWatchingResource lockWatcher = EteSetup.createClientToSingleNode(LockWatchingResource.class);

    @Test
    public void test() {
        lockWatcher.register("test");
        Map<LockDescriptor, LockWatch> result = lockWatcher.getWatches().asMap();
        assertThat(lockWatcher.getWatches().asMap()).containsExactly(entry(getLockDescriptor("test"),
                LockWatch.INVALID));
        lockWatcher.deregister("test");
    }

    @Test
    public void test2() {
        lockWatcher.register("test");
        LockWatch lockWatch = lockWatcher.getWatches().asMap().get(getLockDescriptor("test"));
        long timestamp = lockWatch.timestamp();
        assertThat(lockWatch.fromCommittedTransaction()).isFalse();
        assertThat(timestamp).isGreaterThan(LockWatch.INVALID.timestamp());

//        lockWatcher.unlock(token);
        lockWatch = lockWatcher.getWatches().asMap().get(getLockDescriptor("test"));
        assertThat(lockWatch.fromCommittedTransaction()).isTrue();
        assertThat(lockWatch.timestamp()).isEqualTo(timestamp);
        lockWatcher.deregister("test");
    }

    @Test
    public void test3() {
        lockWatcher.register("test");
        lockWatcher.transaction("test");
        LockWatch lockWatch = lockWatcher.getWatches().asMap().get(getLockDescriptor("test"));
        assertThat(lockWatch.fromCommittedTransaction()).isTrue();
        assertThat(lockWatch.timestamp()).isGreaterThan(LockWatch.INVALID.timestamp());
        lockWatcher.deregister("test");
    }

    public LockDescriptor getLockDescriptor(String row) {
        return AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes(row));
    }
}
