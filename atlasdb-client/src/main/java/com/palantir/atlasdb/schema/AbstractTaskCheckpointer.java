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
package com.palantir.atlasdb.schema;

import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.Map;
import javax.annotation.Nullable;

public abstract class AbstractTaskCheckpointer {
    private static final byte INCOMPLETE = 0;
    private static final byte COMPLETE = 1;

    protected final TransactionManager txManager;

    public AbstractTaskCheckpointer(TransactionManager txManager) {
        this.txManager = txManager;
    }

    /**
     * Update the checkpoint value with the given nextRowName. If nextRowName is the empty byte
     * array, it represents a completed checkpoint.
     *
     * This doesn't make much sense unless this is called within a transaction, because that
     * transaction could be retried.
     */
    public abstract void checkpoint(String extraId, long rangeId, byte[] nextRowName, Transaction tx);

    /**
     * Get the next row name for this range. Returns null if the checkpoint is done.
     */
    public abstract @Nullable byte[] getCheckpoint(String extraId, long rangeId, Transaction tx);

    /**
     * Initialize checkpointing. This will only write checkpoints if checkpoints don't already
     * exist.
     *
     * Checkpoints written here are always valid.
     */
    public abstract void createCheckpoints(String extraId, Map<Long, byte[]> startById);

    /**
     * Deletes the checkpoints for this checkpointer. This is only to be called once the task is
     * complete and the caller knows that these checkpoints never need to be read again.
     */
    public abstract void deleteCheckpoints();

    /**
     * Prepends the byte array with a value that indicates if this range is complete.
     */
    protected byte[] toDb(byte[] bytes, boolean alwaysValid) {
        byte[] ret = new byte[bytes.length + 1];
        System.arraycopy(bytes, 0, ret, 1, bytes.length);
        if (!alwaysValid && bytes.length == 0) {
            ret[0] = COMPLETE;
        } else {
            ret[0] = INCOMPLETE;
        }
        return ret;
    }

    /**
     * Removes the indicator from the byte array and returns null if it is complete.
     */
    protected @Nullable byte[] fromDb(byte[] bytes) {
        if (bytes[0] == COMPLETE) {
            return null;
        }
        byte[] ret = new byte[bytes.length - 1];
        System.arraycopy(bytes, 1, ret, 0, ret.length);
        return ret;
    }
}
