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

import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface ExpectationsConfig {
    static final long MAXIMUM_NAME_SIZE = 255;

    /**
     * Length should not exceed {@value #MAXIMUM_NAME_SIZE}.
     * This will be used for logging and will be expected to be safe to log.
     */
    Optional<String> transactionName();

    long transactionAgeMillisLimit();

    long bytesReadLimit();

    long bytesReadInOneKvsCallLimit();

    long kvsReadCallCountLimit();
}
