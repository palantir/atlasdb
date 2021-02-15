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
package com.palantir.atlasdb.performance.benchmarks.table;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class VeryWideRowTable extends WideRowTable {
    @Override
    public TableReference getTableRef() {
        return TableReference.createFromFullyQualifiedName("performance.persistent_very_wide");
    }

    @Override
    public int getNumCols() {
        return 1_000_000;
    }

    @Override
    public boolean isPersistent() {
        return true;
    }
}
