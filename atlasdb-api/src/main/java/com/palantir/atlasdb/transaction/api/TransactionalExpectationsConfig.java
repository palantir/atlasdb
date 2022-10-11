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

package com.palantir.atlasdb.transaction.api;

import com.google.common.base.Preconditions;
import org.immutables.value.Value;

@Value.Immutable
public class TransactionalExpectationsConfig {
    @Value.Default
    long maximumBytesRead() {
        return 10737418240L;
    }
    ;

    @Value.Default
    long maximumBytesReadInOneCall() {
        return 5368709120L;
    }
    ;

    @Value.Default
    long maximumReadCallsMade() {
        return 1000L;
    }
    ;

    @Value.Check
    protected void check() {
        Preconditions.checkArgument(maximumBytesRead() > 0, "'maximumBytesRead' should be strictly positive");
        Preconditions.checkArgument(
                maximumBytesReadInOneCall() > 0, "'maximumBytesReadInOneCall' should be strictly positive");
        Preconditions.checkArgument(maximumReadCallsMade() > 0, "'maximumReadCallsMade' should be strictly positive");
    }
}
