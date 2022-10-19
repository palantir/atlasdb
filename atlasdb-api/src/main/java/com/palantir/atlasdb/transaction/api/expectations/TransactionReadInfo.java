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

import com.palantir.atlasdb.transaction.api.KvsCallReadInfo;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Data about a transaction's read/get interactions with {@link com.palantir.atlasdb.keyvalue.api.KeyValueService}.
 */
@Value.Immutable
public interface TransactionReadInfo {
    long bytesRead();

    long kvsCalls();

    /**
     * Data about the {@link com.palantir.atlasdb.keyvalue.api.KeyValueService} read/get call that read the
     * most amount of data  for a given transaction.
     */
    Optional<KvsCallReadInfo> maximumBytesKvsCallInfo();
}
