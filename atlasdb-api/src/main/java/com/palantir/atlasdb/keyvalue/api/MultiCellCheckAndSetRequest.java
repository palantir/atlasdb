/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/**
 * See also {@link CheckAndSetRequest}. This object is a parameter object for
 * {@link KeyValueService#checkAndSet(MultiCellCheckAndSetRequest)}.
 *
 * Note that individual key-value service may not support a fully general request, because the guarantees of the API
 * require atomicity across updates and the underlying key-value services may not always provide that (e.g., across
 * partitions).
 */
@Value.Immutable
public interface MultiCellCheckAndSetRequest {
    TableReference tableReference();

    List<ProposedUpdate> proposedUpdates();

    @Value.Lazy
    default boolean isSingleRowScoped() {
        int uniqueRows = proposedUpdates().stream()
                .map(ProposedUpdate::cell)
                .map(Cell::getRowName)
                .collect(Collectors.toSet())
                .size();
        return uniqueRows == 1;
    }

    @Value.Lazy
    default Map<Cell, byte[]> expectations() {
        return KeyedStream.of(proposedUpdates())
                .mapKeys(ProposedUpdate::cell)
                .map(ProposedUpdate::oldValue)
                .collectToMap();
    }

    @Value.Lazy
    default Map<Cell, byte[]> updates() {
        return KeyedStream.of(proposedUpdates())
                .mapKeys(ProposedUpdate::cell)
                .map(ProposedUpdate::newValue)
                .collectToMap();
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(
                proposedUpdates().stream()
                                .map(ProposedUpdate::cell)
                                .collect(Collectors.toSet())
                                .size()
                        == proposedUpdates().size(),
                "Proposed updates must apply to unique cells only");
    }

    @Value.Immutable
    interface ProposedUpdate {
        Cell cell();

        byte[] oldValue();

        byte[] newValue();
    }
}
