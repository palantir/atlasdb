package com.palantir.atlasdb.schema;

import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.ptobject.EncodingUtils.EncodingType;

public class RowTransformers {

    private RowTransformers() {
        // cannot instantiate
    }

    public static class RowComponent {
        private final int index;
        private final EncodingType type;

        public RowComponent(int index, EncodingType type) {
            this.index = index;
            this.type = type;
        }

        public static RowComponent of(int index, EncodingType type) {
            return new RowComponent(index, type);
        }

        public int getIndex() {
            return index;
        }

        public EncodingType getType() {
            return type;
        }

        public static final Function<RowComponent, Integer> TO_INDEX = new Function<RowComponent, Integer>() {
            @Override
            public Integer apply(RowComponent component) {
                return component.getIndex();
            }
        };

        public static final Function<RowComponent, EncodingType> TO_TYPE = new Function<RowComponent, EncodingType>() {
            @Override
            public EncodingType apply(RowComponent component) {
                return component.getType();
            }
        };
    }

    /**
     * Returns a function that permutes the order of the components of each row without changing their types.
     */
    public static Function<byte[], byte[]> rowNamePermutation(List<EncodingType> initialTypes, List<Integer> permutation) {
        Preconditions.checkArgument(initialTypes.size() == permutation.size());
        List<RowComponent> fullPermutation = Lists.newArrayListWithCapacity(permutation.size());
        for (int i = 0; i < initialTypes.size(); i++) {
            fullPermutation.add(RowComponent.of(permutation.get(i), initialTypes.get(permutation.get(i))));
        }
        return rowNamePermuformation(initialTypes, fullPermutation);
    }

    /**
     * Returns a function that changes the type of the components in each row without changing their order.
     */
    public static Function<byte[], byte[]> rowNameTransformation(List<EncodingType> initialTypes, List<EncodingType> finalTypes) {
        Preconditions.checkArgument(initialTypes.size() == finalTypes.size());
        List<RowComponent> fullPermutation = Lists.newArrayListWithCapacity(initialTypes.size());
        for (int i = 0; i < initialTypes.size(); i++) {
            fullPermutation.add(RowComponent.of(i, finalTypes.get(i)));
        }
        return rowNamePermuformation(initialTypes, fullPermutation);
    }

    /**
     * Returns a function that changes the type and ordering of the components in each row.
     */
    public static Function<byte[], byte[]> rowNamePermuformation(final List<EncodingType> initialTypes, List<RowComponent> permutation) {
        Preconditions.checkArgument(initialTypes.size() == permutation.size());
        Set<Integer> outputIndices = ImmutableSet.copyOf(Iterables.transform(permutation, RowComponent.TO_INDEX));
        Preconditions.checkArgument(outputIndices.size() == permutation.size(), "Permutation maps multiple components to the same index.");
        for (int index : outputIndices) {
            Preconditions.checkElementIndex(index, permutation.size());
        }

        final List<Integer> indexPermutation = ImmutableList.copyOf(Lists.transform(permutation, RowComponent.TO_INDEX));
        final List<EncodingType> finalTypes = ImmutableList.copyOf(Lists.transform(permutation, RowComponent.TO_TYPE));
        return new Function<byte[], byte[]>() {
            @Override
            public byte[] apply(byte[] input) {
                List<Object> initialValues = EncodingUtils.fromBytes(input, initialTypes);
                List<Object> finalValues = Lists.newArrayListWithCapacity(indexPermutation.size());
                for (int i = 0; i < indexPermutation.size(); i++) {
                    finalValues.add(initialValues.get(indexPermutation.get(i)));
                }
                return EncodingUtils.toBytes(finalTypes, finalValues);
            }
        };
    }
}
