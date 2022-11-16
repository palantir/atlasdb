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

package com.palantir.atlasdb.atomic.mcas;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.logsafe.Preconditions;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface CasRequest {
    @Value.Parameter
    Cell cell();

    @Value.Parameter
    ByteBuffer expected();

    @Value.Parameter
    ByteBuffer update();

    @Value.Derived
    default UpdateRank rank() {
        if (expected().equals(update())) {
            return UpdateRank.TOUCH;
        }
        if (update().equals(MarkAndCasConsensusForgettingStore.WRAPPED_ABORTED_TRANSACTION_STAGING_VALUE)) {
            return UpdateRank.ABORT;
        }
        return UpdateRank.COMMIT;
    }

    static Exception failure(CasRequest req, Optional<byte[]> actual) {
        return failureInternal(req, actual);
    }

    static Exception failureUntried(CasRequest req) {
        return failureInternal(req, Optional.empty());
    }

    private static Exception failureInternal(CasRequest req, Optional<byte[]> actual) {
        return req.rank().equals(UpdateRank.TOUCH)
                ? new CheckAndSetException(
                        "Atomic update cannot go through as the expected value for the key does not match the actual "
                                + "value.",
                        req.cell(),
                        req.expected().array(),
                        actual.map(ImmutableList::of).orElseGet(ImmutableList::of))
                : new KeyAlreadyExistsException(
                        "Atomic update cannot go through as the key already exists in the KVS.",
                        ImmutableList.of(req.cell()));
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(expected().hasArray(), "Cannot request CAS without expected value.");
        Preconditions.checkState(update().hasArray(), "Cannot request CAS without update.");
    }
}
