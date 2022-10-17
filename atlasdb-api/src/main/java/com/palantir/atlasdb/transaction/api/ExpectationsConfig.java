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
import com.palantir.logsafe.SafeArg;
import java.time.Duration;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExpectationsConfig {
    public static final String DEFAULT_TRANSACTION_NAME = "<un-named>";
    public static final long MAXIMUM_NAME_SIZE = 255;
    public static final long ONE_GIBIBYTE = mebibytesToBytes(1024);

    /**
     * Length should not exceed {@value #MAXIMUM_NAME_SIZE}
     */
    public Optional<String> transactionName() {
        return Optional.empty();
    }

    @Value.Default
    public long transactionAgeMillisLimit() {
        return Duration.ofHours(3).toMillis();
    }

    @Value.Default
    public long bytesReadLimit() {
        return 10 * ONE_GIBIBYTE;
    }

    @Value.Default
    public long bytesReadInOneKvsCallLimit() {
        return 5 * ONE_GIBIBYTE;
    }

    @Value.Default
    public long kvsReadCallCountLimit() {
        return 100L;
    }

    @Value.Check
    protected void check() {
        transactionName().ifPresent(name -> {
            Preconditions.checkArgument(
                    name.length() <= MAXIMUM_NAME_SIZE,
                    "transactionName should be shorter",
                    SafeArg.of("maximumNameSize", MAXIMUM_NAME_SIZE),
                    SafeArg.of("transactionalExpectationsConfig", this));
        });

        Preconditions.checkArgument(
                transactionAgeMillisLimit() > 0,
                "transactionAgeMillisLimit should be strictly positive.",
                SafeArg.of("transactionalExpectationsConfig", this));
        Preconditions.checkArgument(
                bytesReadLimit() > 0,
                "bytesReadLimit should be strictly positive",
                SafeArg.of("transactionalExpectationsConfig", this));
        Preconditions.checkArgument(
                bytesReadInOneKvsCallLimit() > 0,
                "bytesReadInOneKvsCallLimit should be strictly positive",
                SafeArg.of("transactionalExpectationsConfig", this));
        Preconditions.checkArgument(
                kvsReadCallCountLimit() > 0,
                "kvsReadCallCountLimit should be strictly positive",
                SafeArg.of("transactionalExpectationsConfig", this));
    }

    public static long mebibytesToBytes(long mebibytes) {
        return 1024 * 1024 * mebibytes;
    }
}
