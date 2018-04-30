/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb;

public final class AtlasDbPerformanceConstants {
    private AtlasDbPerformanceConstants() {
        // Utility class
    }

    /**
     * If a user passes in a huge page size for a batch for a range scan, we won't actually make the page size any
     * bigger than {@value #MAX_BATCH_SIZE}.
     */
    public static final int MAX_BATCH_SIZE = 10000;

    public static final int MAX_BATCH_SIZE_BYTES = 10 * 1024 * 1024;
}
