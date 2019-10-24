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

package com.palantir.atlasdb.transaction.impl;

import com.google.common.base.Preconditions;

interface SynchronousTracker extends AutoCloseable {
    SynchronousTracker NO_OP = new SynchronousTracker() {
        @Override
        public SynchronousTracker enterAsyncCall() {
            return this;
        }

        @Override
        public SynchronousTracker exitAsyncCall() {
            return this;
        }

        @Override
        public void checkInAsync() {

        }

        @Override
        public void checkInSync() {

        }
    };

    static SynchronousTracker constructSynchronousTracker() {
        return new SynchronousTracker() {
            volatile boolean inAsyncBoolean;

            @Override
            public SynchronousTracker enterAsyncCall() {
                inAsyncBoolean = true;
                return this;
            }

            @Override
            public SynchronousTracker exitAsyncCall() {
                inAsyncBoolean = false;
                return this;
            }

            @Override
            public void checkInAsync() {
                Preconditions.checkState(inAsyncBoolean, "Not expected to be in sync path");
            }

            @Override
            public void checkInSync() {
                Preconditions.checkState(inAsyncBoolean, "Not expected to be in async path");
            }
        };
    }

    SynchronousTracker enterAsyncCall();

    SynchronousTracker exitAsyncCall();

    void checkInAsync();

    void checkInSync();

    @Override
    default void close() {
        exitAsyncCall();
    }
}
