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
package com.palantir.lock;

import com.google.common.base.Strings;
import com.palantir.logsafe.Preconditions;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A descriptor for a {@link ReadWriteLock}, identified by a lock ID (a unique
 * string).
 *
 * @author jtamer
 */
public final class AtlasCellLockDescriptor {

    private AtlasCellLockDescriptor() {
        // cannot instantiate
    }

    /** Returns a {@code LockDescriptor} instance for the given table, row, and column. */
    public static LockDescriptor of(String tableName, byte[] rowName, byte[] colName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));
        Preconditions.checkNotNull(rowName, "rowName should not be null");
        Preconditions.checkNotNull(colName, "colName should not be null");
        byte[] tableBytes = tableName.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[tableBytes.length + 1 + rowName.length + 1 + colName.length];
        System.arraycopy(tableBytes, 0, bytes, 0, tableBytes.length);
        System.arraycopy(rowName, 0, bytes, tableBytes.length + 1, rowName.length);
        System.arraycopy(colName, 0, bytes, tableBytes.length + 1 + rowName.length + 1, colName.length);
        return new LockDescriptor(bytes);
    }
}
