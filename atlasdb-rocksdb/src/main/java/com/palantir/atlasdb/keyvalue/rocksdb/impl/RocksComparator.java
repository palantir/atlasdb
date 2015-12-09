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

import java.util.Arrays;

import org.rocksdb.Comparator;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Slice;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.ptobject.EncodingUtils;

public class RocksComparator extends Comparator {
    public static final RocksComparator INSTANCE = new RocksComparator(new ComparatorOptions());

    public RocksComparator(ComparatorOptions copt) {
        super(copt);
    }

    @Override
    public String name() {
        return "atlasdb-v2";
    }

    // This method is a hotspot, logic from RocksDbKeyValueServices.parseCellAndTs
    // is duplicated and tuned for perf.
    @Override
    public int compare(Slice a, Slice b) {
        byte[] adata = a.data();
        byte[] bdata = b.data();
        byte[] rowSizeBytes = new byte[2];
        rowSizeBytes[0] = adata[adata.length - 1];
        rowSizeBytes[1] = adata[adata.length - 2];
        int aRowSize = (int) EncodingUtils.decodeVarLong(rowSizeBytes);
        rowSizeBytes[0] = bdata[bdata.length - 1];
        rowSizeBytes[1] = bdata[bdata.length - 2];
        int bRowSize = (int) EncodingUtils.decodeVarLong(rowSizeBytes);

        int comp = UnsignedBytes.lexicographicalComparator().compare(Arrays.copyOf(adata, aRowSize),
                                                                     Arrays.copyOf(bdata, bRowSize));
        if (comp != 0) {
            return comp;
        }

        int aColEnd = adata.length - 8 - EncodingUtils.sizeOfVarLong(aRowSize);
        int bColEnd = bdata.length - 8 - EncodingUtils.sizeOfVarLong(bRowSize);
        comp = UnsignedBytes.lexicographicalComparator().compare(Arrays.copyOfRange(adata, aRowSize, aColEnd),
                                                                 Arrays.copyOfRange(bdata, bRowSize, bColEnd));
        if (comp != 0) {
            return comp;
        }

        long aTs = Longs.fromBytes(
                adata[aColEnd+0],
                adata[aColEnd+1],
                adata[aColEnd+2],
                adata[aColEnd+3],
                adata[aColEnd+4],
                adata[aColEnd+5],
                adata[aColEnd+6],
                adata[aColEnd+7]);
        long bTs = Longs.fromBytes(
                bdata[bColEnd+0],
                bdata[bColEnd+1],
                bdata[bColEnd+2],
                bdata[bColEnd+3],
                bdata[bColEnd+4],
                bdata[bColEnd+5],
                bdata[bColEnd+6],
                bdata[bColEnd+7]);

        // Note: Ordering is reversed, later timestamps come before eariler ones.
        return Long.compare(bTs, aTs);
    }
}
