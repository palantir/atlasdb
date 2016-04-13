/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.api;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Multimap;

/**
 * An extension of KeyValueService that allows for auto-expiring data.
 * The anticipated usage is allowing easy and highly available persistence of data structures like caches and queues,
 * which perform exceptionally poorly if implemented only through KeyValueService.
 *
 * Contract with the user:
 * - This is for space and deletion time savings only.
 * - This does not fulfill the same goals/legal requirements as hard delete for sensitive data. (though data written by this service can itself be deleted by normal hard delete routines)
 * - // todo Only tables marked with 'AllowsSelfExpiringData' in their schema may use these features
 *
 * - Normal atlasdb guarantees may be broken. Data written with expiration times are:
 * 1. not guaranteed to expire atomically with the entire row or at any other level of granularity
 * 2. not guaranteed to exist at any time after being written (as this feature implicitly requires distributed wall clock time)
 * 3. not guaranteed to be deleted after the specified expiration time
 *
 * That said, points #2 and #3 are attempted on a best-effort basis and should work adequately with synchronized cluster clocks and expiration durations generously outside what is strictly necessary for the application.
 *
 * @author clockfort
 *
 */
public interface ExpiringKeyValueService extends KeyValueService {

    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, final long timestamp, final long time, final TimeUnit unit) throws KeyAlreadyExistsException;
    public void put(final TableReference tableRef, final Map<Cell, byte[]> values, final long timestamp, final long time, final TimeUnit unit);
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values, final long time, final TimeUnit unit);

}
