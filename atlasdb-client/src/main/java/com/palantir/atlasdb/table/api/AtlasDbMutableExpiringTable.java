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
package com.palantir.atlasdb.table.api;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.Multimap;

/*
 * Each AtlasDbTable should implement this interface.
 */
public interface AtlasDbMutableExpiringTable<ROW, COLUMN_VALUE, ROW_RESULT> extends
            AtlasDbImmutableTable<ROW, COLUMN_VALUE, ROW_RESULT> {
    void put(Multimap<ROW, ? extends COLUMN_VALUE> rows, long duration, TimeUnit durationTimeUnit);
    void putUnlessExists(Multimap<ROW, ? extends COLUMN_VALUE> rows, long duration, TimeUnit durationTimeUnit);
}
