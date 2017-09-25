/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.schema.constraints.foreign;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.table.description.constraints.ForeignKeyConstraint;
import com.palantir.atlasdb.table.description.constraints.tuples.TupleOf2;

public class Select2Long implements ForeignKeyConstraint {
    public static List<TupleOf2<Long, Long>> getKeys(Long param, Long param2) {
        return ImmutableList.of(TupleOf2.of(param, param2));
    }
}
