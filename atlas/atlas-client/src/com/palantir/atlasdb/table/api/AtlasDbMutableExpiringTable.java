// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.table.api;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.Multimap;

/*
 * Each AtlasDbTable should implement this interface.
 */
public interface AtlasDbMutableExpiringTable<ROW, COLUMN_VALUE, ROW_RESULT> extends
            AtlasDbImmutableTable<ROW, COLUMN_VALUE, ROW_RESULT> {
    public void put(Multimap<ROW, ? extends COLUMN_VALUE> rows, long duration, TimeUnit durationTimeUnit);
}
