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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import java.util.Arrays;
import java.util.List;

public class ExplicitRowNamePartitioner implements RowNamePartitioner {
    final ValueType valueType;
    final ImmutableSet<String> values;

    public ExplicitRowNamePartitioner(ValueType valueType, Iterable<String> values) {
        this.valueType = valueType;
        this.values = ImmutableSet.copyOf(values);
    }

    @Override
    public boolean isHotSpot() {
        return true;
    }

    @Override
    public List<byte[]> getPartitions(int numberRanges) {
        return getPartitionsInternal();
    }

    private List<byte[]> getPartitionsInternal() {
        List<byte[]> ret = Lists.newArrayList();
        for (String value : values) {
            ret.add(valueType.convertFromString(value));
        }
        return ret;
    }

    @Override
    public List<RowNamePartitioner> compound(RowNamePartitioner next) {
        List<byte[]> tokens = getPartitionsInternal();
        List<RowNamePartitioner> ret = Lists.newArrayList();
        for (byte[] bs : tokens) {
            ret.add(new CompoundRowNamePartitioner(bs, next));
        }
        return ret;
    }

    static class CompoundRowNamePartitioner implements RowNamePartitioner {
        final byte[] prefix;
        final RowNamePartitioner nextPartition;

        CompoundRowNamePartitioner(byte[] prefix, RowNamePartitioner nextPartition) {
            this.prefix = prefix;
            this.nextPartition = nextPartition;
        }

        @Override
        public List<byte[]> getPartitions(int numberRanges) {
            List<byte[]> ret = Lists.newArrayList();
            for (byte[] bs : nextPartition.getPartitions(numberRanges)) {
                ret.add(Bytes.concat(prefix, bs));
            }
            return ret;
        }

        @Override
        public boolean isHotSpot() {
            return nextPartition.isHotSpot();
        }

        @Override
        public List<RowNamePartitioner> compound(RowNamePartitioner next2) {
            List<RowNamePartitioner> compound = nextPartition.compound(next2);
            List<RowNamePartitioner> ret = Lists.newArrayList();
            for (RowNamePartitioner p : compound) {
                ret.add(new CompoundRowNamePartitioner(prefix, p));
            }
            return ret;
        }

        @Override
        public String toString() {
            return "CompoundRowNamePartitioner [prefix=" + Arrays.toString(prefix)
                    + ", nextPartition=" + nextPartition + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((nextPartition == null) ? 0 : nextPartition.hashCode());
            result = prime * result + Arrays.hashCode(prefix);
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
            CompoundRowNamePartitioner other = (CompoundRowNamePartitioner) obj;
            if (nextPartition == null) {
                if (other.nextPartition != null) {
                    return false;
                }
            } else if (!nextPartition.equals(other.nextPartition)) {
                return false;
            }
            if (!Arrays.equals(prefix, other.prefix)) {
                return false;
            }
            return true;
        }

    }

    @Override
    public String toString() {
        return "ExplicitRowNamePartitioner [valueType=" + valueType + ", values=" + values + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((valueType == null) ? 0 : valueType.hashCode());
        result = prime * result + ((values == null) ? 0 : values.hashCode());
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
        ExplicitRowNamePartitioner other = (ExplicitRowNamePartitioner) obj;
        if (valueType != other.valueType) {
            return false;
        }
        if (values == null) {
            if (other.values != null) {
                return false;
            }
        } else if (!values.equals(other.values)) {
            return false;
        }
        return true;
    }

}
