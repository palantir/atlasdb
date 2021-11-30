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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * Indicates to other Cassandra operations what consistency read operations should be performed at.
 * Notice that this may differ by table.
 */
public final class ReadConsistencyProvider {
    private static final SafeLogger log = SafeLoggerFactory.get(ReadConsistencyProvider.class);

    private static final ConsistencyLevel DEFAULT_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM;

    private final AtomicReference<ConsistencyLevel> defaultReadConsistency = new AtomicReference<>(DEFAULT_CONSISTENCY);

    public ConsistencyLevel getConsistency(TableReference tableReference) {
        if (AtlasDbConstants.SERIAL_CONSISTENCY_ATOMIC_TABLES.contains(tableReference)) {
            return ConsistencyLevel.LOCAL_SERIAL;
        }
        return defaultReadConsistency.get();
    }

    public void lowerConsistencyLevelToOne() {
        if (defaultReadConsistency.get() != ConsistencyLevel.ONE) {
            boolean update = defaultReadConsistency.compareAndSet(DEFAULT_CONSISTENCY, ConsistencyLevel.ONE);
            if (update) {
                log.info(
                        "Lowering read consistency level to ONE.",
                        SafeArg.of("originalReadConsistency", DEFAULT_CONSISTENCY),
                        new SafeRuntimeException("I exist to show you the stack trace."));
            } else {
                log.info(
                        "Could not lower read consistency level to ONE.",
                        SafeArg.of("currentReadConsistency", defaultReadConsistency.get()),
                        SafeArg.of("defaultReadConsistency", DEFAULT_CONSISTENCY),
                        new SafeRuntimeException("I exist to show you the stack trace."));
            }
        } else {
            log.info("Did not lower read consistency to ONE because it was already ONE.");
        }
    }
}
