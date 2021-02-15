/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.history.utils;

import static com.palantir.timelock.TimelockCorruptionTestConstants.DEFAULT_NAMESPACE_AND_USE_CASE;

import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.timelock.history.HistoryQuery;
import com.palantir.timelock.history.HistoryQuerySequenceBounds;

public class HistoryQueries {

    public static HistoryQuery unboundedHistoryQuerySinceSeq(long seqLowerBound) {
        return unboundedHistoryQuerySinceSeqForNamespaceAndUseCase(DEFAULT_NAMESPACE_AND_USE_CASE, seqLowerBound);
    }

    public static HistoryQuery unboundedHistoryQuerySinceSeqForNamespaceAndUseCase(
            NamespaceAndUseCase namespaceAndUseCase, long seqLowerBound) {
        return HistoryQuery.of(
                namespaceAndUseCase, HistoryQuerySequenceBounds.of(seqLowerBound, seqLowerBound + 10001));
    }
}
