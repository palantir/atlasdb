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
package com.palantir.atlasdb.table.description;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import java.util.ArrayList;
import java.util.List;

public class UniformRowNamePartitioner implements RowNamePartitioner {
    final ValueType valueType;

    public UniformRowNamePartitioner(ValueType valueType) {
        this.valueType = valueType;
    }

    private boolean isVarLong() {
        return valueType == ValueType.VAR_LONG;
    }

    private boolean isNullable() {
        return valueType == ValueType.NULLABLE_FIXED_LONG;
    }

    @Override
    public List<byte[]> getPartitions(int numberRanges) {
        if (numberRanges <= 1) {
            return ImmutableList.of(new byte[] {0});
        }
        int highestOne = Integer.highestOneBit(numberRanges);
        if (highestOne != numberRanges) {
            numberRanges = highestOne * 2;
        }
        switch (valueType) {
            case VAR_LONG:
            case FIXED_LONG:
            case BLOB:
            case SHA256HASH:
            case FIXED_LONG_LITTLE_ENDIAN:
            case NULLABLE_FIXED_LONG:
            case UUID:
                return getRangesForLongs(numberRanges);
            default:
                throw new UnsupportedOperationException(
                        "AtlasDB does not yet support this type. (But can if you need it)");
        }
    }

    public static boolean allowsUniformPartitioner(ValueType type) {
        switch (type) {
            case VAR_LONG:
            case FIXED_LONG:
            case BLOB:
            case SHA256HASH:
            case FIXED_LONG_LITTLE_ENDIAN:
            case NULLABLE_FIXED_LONG:
            case UUID:
                return true;
            default:
                return false;
        }
    }

    /**
     * numberRanges must be a power of 2 greater than 1.
     */
    private List<byte[]> getRangesForLongs(int numberRanges) {
        int shift = Integer.numberOfTrailingZeros(numberRanges);
        if (!isVarLong()) {
            shift--;
        }
        long increment = Long.MIN_VALUE >>> shift;
        List<byte[]> ret = new ArrayList<>();
        for (int i = 0; i < numberRanges; i++) {
            long val = increment * i;
            if (isVarLong()) {
                if (val == 0L) {
                    ret.add(PtBytes.EMPTY_BYTE_ARRAY);
                } else {
                    ret.add(valueType.convertFromJava(val));
                }
            } else if (isNullable()) {
                if (val == 0L) {
                    ret.add(valueType.convertFromJava(null));
                } else {
                    ret.add(valueType.convertFromJava(val));
                }
            } else {
                ret.add(ValueType.FIXED_LONG.convertFromJava(val));
            }
        }
        return ret;
    }

    @Override
    public boolean isHotSpot() {
        return false;
    }

    @Override
    public List<RowNamePartitioner> compound(RowNamePartitioner next) {
        return ImmutableList.<RowNamePartitioner>of(this);
    }

    @Override
    public String toString() {
        return "UniformRowNamePartitioner [valueType=" + valueType + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((valueType == null) ? 0 : valueType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UniformRowNamePartitioner other = (UniformRowNamePartitioner) obj;
        if (valueType != other.valueType) {
            return false;
        }
        return true;
    }
}
