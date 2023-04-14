/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.workflow.ring;

import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.SafeLoggable;
import java.util.List;
import java.util.Map;

public final class RingValidationException extends IllegalArgumentException implements SafeLoggable {

    private final Map<Integer, Integer> ring;
    private final Type type;

    private RingValidationException(Map<Integer, Integer> ring, Type type) {
        this.ring = ring;
        this.type = type;
    }

    @Override
    public @Safe String getLogMessage() {
        return "Detected an error in the graph when attempting to validate as a ring.";
    }

    @Override
    public List<Arg<?>> getArgs() {
        return List.of(SafeArg.of("ring", ring), SafeArg.of("type", type));
    }

    public static void throwEarlyCycle(Map<Integer, Integer> ring) throws RingValidationException {
        throw new RingValidationException(ring, Type.EARLY_CYCLE);
    }

    public static void throwMissingEntries(Map<Integer, Integer> ring) throws RingValidationException {
        throw new RingValidationException(ring, Type.MISSING_ENTRIES);
    }

    enum Type {
        // Detected a cycle in our ring before visiting all nodes,
        // indicating the integrity of our ring has been compromised.
        EARLY_CYCLE,

        // Detected a reference to a node that does not exist,
        // indicating the integrity of our ring has been compromised.
        MISSING_ENTRIES;
    }
}
