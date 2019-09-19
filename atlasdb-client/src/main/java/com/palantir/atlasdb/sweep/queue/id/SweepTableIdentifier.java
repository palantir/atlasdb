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
package com.palantir.atlasdb.sweep.queue.id;

import static com.palantir.logsafe.Preconditions.checkState;

import com.google.common.primitives.Ints;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.common.persist.Persistable;
import org.immutables.value.Value;

@Value.Immutable
public interface SweepTableIdentifier extends Persistable {
    Hydrator<SweepTableIdentifier> BYTES_HYDRATOR = input -> {
        int identifier = Ints.checkedCast(EncodingUtils.decodeSignedVarLong(input));
        if (identifier < 0) {
            return ImmutableSweepTableIdentifier.builder()
                    .isPending(true)
                    .identifier(-identifier)
                    .build();
        } else {
            return ImmutableSweepTableIdentifier.builder()
                    .isPending(false)
                    .identifier(identifier)
                    .build();
        }
    };

    @Value.Parameter
    int identifier();

    @Value.Parameter
    boolean isPending();

    @Override
    default byte[] persistToBytes() {
        return EncodingUtils.encodeSignedVarLong((isPending() ? -1 : 1) * identifier());
    }

    @Value.Check
    default void check() {
        checkState(identifier() != 0, "Identifier must not be zero");
    }

    static SweepTableIdentifier pending(int identifier) {
        return ImmutableSweepTableIdentifier.of(identifier, true);
    }

    static SweepTableIdentifier identified(int identifier) {
        return ImmutableSweepTableIdentifier.of(identifier, false);
    }
}
