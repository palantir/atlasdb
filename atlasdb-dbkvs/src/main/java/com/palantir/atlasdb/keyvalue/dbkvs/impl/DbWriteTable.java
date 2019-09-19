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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public interface DbWriteTable {
    void put(Collection<Map.Entry<Cell, byte[]>> data, long ts);
    void put(Collection<Map.Entry<Cell, Value>> data);
    void putSentinels(Iterable<Cell> cells);
    void update(Cell cell, long ts, byte[] oldValue, byte[] newValue);
    void delete(List<Entry<Cell, Long>> partition);
    void delete(RangeRequest range);
    void deleteAllTimestamps(Map<Cell, TimestampRangeDelete> deletes);
}
