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

package com.palantir.atlasdb.internalschema;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadOnlyTransactionSchemaManager {
    private static final Logger log = LoggerFactory.getLogger(ReadOnlyTransactionSchemaManager.class);

    private final CoordinationService<InternalSchemaMetadata> coordinationService;

    public ReadOnlyTransactionSchemaManager(CoordinationService<InternalSchemaMetadata> coordinationService) {
        this.coordinationService = coordinationService;
    }

    public Integer getTransactionsSchemaVersion(long timestamp) {
        if (timestamp < AtlasDbConstants.STARTING_TS) {
            throw new SafeIllegalStateException(
                    "Query attempted for timestamp {} which was never given out by the"
                            + " timestamp service, as timestamps start at {}",
                    SafeArg.of("queriedTimestamp", timestamp),
                    SafeArg.of("startOfTime", AtlasDbConstants.STARTING_TS));
        }
        Optional<Integer> possibleVersion =
                extractTimestampVersion(coordinationService.getValueForTimestamp(timestamp), timestamp);
        return possibleVersion.orElse(null);
    }

    private static Optional<Integer> extractTimestampVersion(
            Optional<ValueAndBound<InternalSchemaMetadata>> valueAndBound, long timestamp) {
        return valueAndBound
                .flatMap(ValueAndBound::value)
                .map(InternalSchemaMetadata::timestampToTransactionsTableSchemaVersion)
                .map(versionMap -> versionMap.getValueForTimestamp(timestamp));
    }
}
