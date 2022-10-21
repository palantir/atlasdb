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

package com.palantir.atlasdb.transaction.api.expectations;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.time.Duration;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface ExpectationsConfig {
    int MAXIMUM_NAME_SIZE = 255;
    String DEFAULT_TRANSACTION_DISPLAY_NAME = "Unnamed";
    long ONE_MEBIBYTE = mebibytesToBytes(1);

    /**
     * Length should not exceed {@value #MAXIMUM_NAME_SIZE}.
     * This will be used for logging and will be expected to be safe to log.
     */
    Optional<String> transactionName();

    @Value.Lazy
    default String transactionDisplayName() {
        return transactionName().orElse(DEFAULT_TRANSACTION_DISPLAY_NAME);
    }

    @Value.Default
    default long transactionAgeMillisLimit() {
        return Duration.ofHours(24).toMillis();
    }

    @Value.Default
    default long bytesReadLimit() {
        return 50 * ONE_MEBIBYTE;
    }

    @Value.Default
    default long bytesReadInOneKvsCallLimit() {
        return 10 * ONE_MEBIBYTE;
    }

    @Value.Default
    default long kvsReadCallCountLimit() {
        return 100L;
    }

    @Value.Check
    default void check() {
        transactionName().ifPresent(ExpectationsConfig::checkTransactionName);
    }

    private static void checkTransactionName(String name) {
        Preconditions.checkArgument(
                name.length() <= MAXIMUM_NAME_SIZE,
                "transactionName should be shorter",
                SafeArg.of("transactionNameLength", name.length()),
                SafeArg.of("maximumNameSize", MAXIMUM_NAME_SIZE));
    }

    static long mebibytesToBytes(long mebibytes) {
        return 1024 * 1024 * mebibytes;
    }
}
