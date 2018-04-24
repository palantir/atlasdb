/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;

public final class OverflowSequenceSupplier implements Supplier<Long> {
    static final int OVERFLOW_ID_CACHE_SIZE = 1000;

    private long currentBatchStartId;
    private int currentIdIndex = OVERFLOW_ID_CACHE_SIZE;
    private final ConnectionSupplier conns;
    private final String tablePrefix;

    private OverflowSequenceSupplier(ConnectionSupplier conns, String tablePrefix) {
        this.conns = conns;
        this.tablePrefix = tablePrefix;
    }

    public static OverflowSequenceSupplier create(ConnectionSupplier conns, String tablePrefix) {
        return new OverflowSequenceSupplier(conns, tablePrefix);
    }

    @Override
    public Long get() {
        if (currentIdIndex < OVERFLOW_ID_CACHE_SIZE) {
            return currentBatchStartId + currentIdIndex++;
        }
        currentBatchStartId = conns.get()
                .selectLongUnregisteredQuery(
                        "SELECT " + tablePrefix + AtlasDbConstants.ORACLE_OVERFLOW_SEQUENCE + ".NEXTVAL FROM DUAL");
        currentIdIndex = 0;
        return currentBatchStartId + currentIdIndex++;
    }
}
