/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.table.description;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;

public enum Mutability {
    // Normal.
    MUTABLE,
    // Cells are written to at most twice: they are written, and then deleted strictly after they are written
    // (blind deletes not allowed).
    WEAK_IMMUTABLE,
    // Cells are written to once and only once, and are never deleted.
    STRONG_IMMUTABLE;

    static final SafeLogger log = SafeLoggerFactory.get(Mutability.class);

    public boolean isAtLeastWeakImmutable() {
        return this == Mutability.WEAK_IMMUTABLE || this == Mutability.STRONG_IMMUTABLE;
    }

    public boolean isAtLeastStrongImmutable() {
        return this == Mutability.STRONG_IMMUTABLE;
    }

    public static Mutability fromProto(TableMetadataPersistence.Mutability mutability) {
        switch (mutability) {
            case MUTABLE:
                return MUTABLE;
            case WEAK_IMMUTABLE:
                return WEAK_IMMUTABLE;
            case STRONG_IMMUTABLE:
                return STRONG_IMMUTABLE;
            default:
                log.warn(
                        "unexpected mutability type, returning MUTABLE because that is safe",
                        new SafeRuntimeException("I exist to show you the stack trace."));
                return MUTABLE;
        }
    }
}
