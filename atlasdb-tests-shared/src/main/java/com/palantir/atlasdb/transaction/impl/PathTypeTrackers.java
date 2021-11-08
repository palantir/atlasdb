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

import com.palantir.logsafe.Preconditions;

public final class PathTypeTrackers {
    private PathTypeTrackers() {
        // static utility class
    }

    public static final PathTypeTracker NO_OP = new PathTypeTracker() {
        @Override
        public PathTypeTracker enterAsyncPath() {
            return this;
        }

        @Override
        public PathTypeTracker exitAsyncPath() {
            return this;
        }

        @Override
        public void expectedToBeInAsync() {}

        @Override
        public void checkNotInAsync() {}
    };

    public static PathTypeTracker constructSynchronousTracker() {
        return new PathTypeTracker() {
            volatile boolean inAsyncBoolean;

            @Override
            public PathTypeTracker enterAsyncPath() {
                inAsyncBoolean = true;
                return this;
            }

            @Override
            public PathTypeTracker exitAsyncPath() {
                inAsyncBoolean = false;
                return this;
            }

            @Override
            public void expectedToBeInAsync() {
                Preconditions.checkState(inAsyncBoolean, "Not expected to be in sync path");
            }

            @Override
            public void checkNotInAsync() {
                Preconditions.checkState(!inAsyncBoolean, "Not expected to be in async path");
            }
        };
    }
}
