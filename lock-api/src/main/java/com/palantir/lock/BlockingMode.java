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

/**
 * Defines how long to block when making a request to the lock server.
 *
 * @author jtamer
 */
public enum BlockingMode {

    /**
     * Instructs the lock server to attempt to immediately acquire the locks
     * without blocking.
     */
    DO_NOT_BLOCK,

    /**
     * Instructs the lock server to block for at most a specified amount of
     * time when attempting to acquire the locks.
     */
    BLOCK_UNTIL_TIMEOUT,

    /**
     * Instructs the lock server to block for as long as necessary when
     * acquiring the locks. This is the default behavior.
     */
    BLOCK_INDEFINITELY,

    /**
     * Instructs the lock server to block for as long as necessary when
     * acquiring the locks, then immediately release them.  This ensures that each lock will have
     * been locked and then released.  This BlockingMode can be used to save round trips to the lock
     * server if you just have to wait for someone to release the lock before you perform a task.
     */
    BLOCK_INDEFINITELY_THEN_RELEASE;
}
