/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;


import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;

public final class OverflowSequenceSupplier {
    static final int OVERFLOW_ID_CACHE_SIZE = 1000;

    private static Supplier<Long> overflowSupplier = null;

    private OverflowSequenceSupplier() {
        //Utility class
    }

    public static synchronized Supplier<Long> getOverflowSeqSupplier(
            ConnectionSupplier conns,
            String tablePrefix) {
        if (overflowSupplier == null) {
            overflowSupplier = new Supplier<Long>() {
                private long currentBatchStartId;
                private int currentIdIndex = OVERFLOW_ID_CACHE_SIZE;

                @Override
                public synchronized Long get() {
                    if (currentIdIndex < OVERFLOW_ID_CACHE_SIZE) {
                        return currentBatchStartId + currentIdIndex++;
                    }
                    currentBatchStartId = conns.get().selectIntegerUnregisteredQuery(
                            "SELECT " + tablePrefix + "OVERFLOW_SEQ.NEXTVAL FROM DUAL");
                    currentIdIndex = 0;
                    return currentBatchStartId + currentIdIndex++;
                }
            };
        }
        return overflowSupplier;
    }
}
