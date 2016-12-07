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
package com.palantir.atlasdb.timelock.jsimpledb;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JTransaction;
import org.jsimpledb.ValidationMode;
import org.jsimpledb.core.ObjId;

import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class JSimpleDbTimestampService implements TimestampService {
    private final JSimpleDB jdb;
    private final ObjId timestampId;

    public JSimpleDbTimestampService(JSimpleDB jdb, String client) {
        this.jdb = jdb;
        this.timestampId = getTimestampId(jdb, client);
    }

    @Override
    public long getFreshTimestamp() {
        return getFreshTimestamps(1).getLowerBound();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return JSimpleDbRetryer.getWithRetry(() -> {
            JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
            JTransaction.setCurrent(tx);
            try {
                Timestamp ts = Timestamp.get(timestampId);
                long lastTimestampHandedOut = ts.getTimestamp();
                ts.setTimestamp(lastTimestampHandedOut + numTimestampsRequested);
                tx.commit();
                return TimestampRange.createInclusiveRange(
                        lastTimestampHandedOut + 1,
                        lastTimestampHandedOut + numTimestampsRequested);
            } finally {
                JTransaction.setCurrent(null);
            }
        });
    }

    private static ObjId getTimestampId(JSimpleDB jdb, String client) {
        return JSimpleDbRetryer.getWithRetry(() -> {
            JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
            JTransaction.setCurrent(tx);
            try {
                ObjId timestampId = Timestamp.getAll().stream()
                        .filter(ts -> ts.getClient().equals(client))
                        .findFirst()
                        .orElseGet(() -> Timestamp.create(client))
                        .getObjId();
                tx.commit();
                return timestampId;
            } finally {
                JTransaction.setCurrent(null);
            }
        });
    }
}
