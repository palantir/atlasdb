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

import com.palantir.logsafe.Preconditions;
import java.time.Duration;
import org.immutables.value.Value;

@Value.Immutable
public abstract class TransactionalExpectationsConfig {
    @Value.Default
    String transactionName() {
        return "<un-named>";
    }

    @Value.Default
    long transactionAgeMillisLimit() {
        return Duration.ofHours(3).toMillis();
    }

    @Value.Default
    long bytesReadLimit() {
        return 10 * ONE_GIBIBYTE;
    }

    @Value.Default
    long bytesReadInOneKvsCallLimit() {
        return 5 * ONE_GIBIBYTE;
    }

    @Value.Default
    long kvsReadCallCountLimit() {
        return 1000L;
    }

    public static final long ONE_GIBIBYTE = mebibytesToBytes(1024);
    public static final long MAXIMUM_NAME_SIZE = 255;

    public static TransactionalExpectationsConfig defaultTransactionalExpectationsConfig() {
        return ImmutableTransactionalExpectationsConfig.builder().build();
    }

    public static long mebibytesToBytes(long mebibytes) {
        return 1024 * 1024 * mebibytes;
    }

    @Value.Check
    protected void check() {
        Preconditions.checkArgument(transactionName().length() <= MAXIMUM_NAME_SIZE, "'transactionName' should be at most " + MAXIMUM_NAME_SIZE);
        Preconditions.checkArgument(transactionAgeMillisLimit() > 0, "'transactionAgeMillisLimit' should be strictly positive.")
        Preconditions.checkArgument(bytesReadLimit() > 0, "'bytesReadLimit' should be strictly positive");
        Preconditions.checkArgument(
                bytesReadInOneKvsCallLimit() > 0, "'bytesReadInOneKvsCallLimit' should be strictly positive");
        Preconditions.checkArgument(kvsReadCallCountLimit() > 0, "'kvsReadCallCountLimit' should be strictly positive");
    }
}
