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
package com.palantir.atlasdb.keyvalue.rocksdb.impl;

import org.rocksdb.Comparator;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Slice;

import com.google.common.collect.ComparisonChain;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.util.Pair;

public class RocksComparator extends Comparator {
    public static final RocksComparator INSTANCE = new RocksComparator(new ComparatorOptions());

    public RocksComparator(ComparatorOptions copt) {
        super(copt);
    }

    @Override
    public String name() {
        return "atlasdb";
    }

    @Override
    public int compare(Slice a, Slice b) {
        Pair<Cell, Long> cellAndTsA = RocksDbKeyValueServices.parseCellAndTs(a.data());
        Pair<Cell, Long> cellAndTsB = RocksDbKeyValueServices.parseCellAndTs(b.data());
        return ComparisonChain.start()
                .compare(cellAndTsA.lhSide.getRowName(), cellAndTsB.lhSide.getRowName(), UnsignedBytes.lexicographicalComparator())
                .compare(cellAndTsA.lhSide.getColumnName(), cellAndTsB.lhSide.getColumnName(), UnsignedBytes.lexicographicalComparator())
                .compare(cellAndTsA.rhSide, cellAndTsB.rhSide)
                .result();
    }
}
