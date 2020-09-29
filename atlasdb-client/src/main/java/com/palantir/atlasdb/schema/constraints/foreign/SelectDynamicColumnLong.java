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
package com.palantir.atlasdb.schema.constraints.foreign;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.table.description.constraints.ForeignKeyConstraint;
import com.palantir.atlasdb.table.description.constraints.tuples.TupleOf1;
import java.util.List;

public class SelectDynamicColumnLong implements ForeignKeyConstraint {
    public static List<TupleOf1<Long>> getKeys(List<Long> longs) {
        List<TupleOf1<Long>> list = Lists.newArrayList();
        for (Long l : longs) {
            list.add(TupleOf1.of(l));
        }
        return list;
    }
}
