/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import static com.palantir.atlasdb.keyvalue.api.TableReference.createFromFullyQualifiedName;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.math.IntMath;
import com.palantir.atlasdb.encoding.PtBytes;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.data.Percentage;
import org.junit.Test;

public class CellReferenceTest {
    private static final TableReference TABLE = createFromFullyQualifiedName("fixed.table");
    private static final byte[] ROW = PtBytes.toBytes("row");
    private static final byte[] COL = PtBytes.toBytes("col");

    @Test
    public void dynamicSequentialColumns() {
        int totalCount = 20_000;
        List<Integer> hashes = IntStream.range(0, totalCount)
                .mapToObj(col -> CellReference.of(TABLE, ROW, PtBytes.toBytes(col)))
                .map(CellReference::goodHash)
                .collect(Collectors.toList());
        assertDistributionUniform(hashes, totalCount);
    }

    @Test
    public void dynamicRandomColumns() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int totalCount = 50_000;
        List<byte[]> cols = IntStream.range(0, totalCount)
                .mapToObj(_ignore -> new byte[random.nextInt(1, 20)])
                .collect(Collectors.toList());
        cols.forEach(random::nextBytes);
        List<Integer> hashes = cols.stream()
                .map(col -> CellReference.of(TABLE, ROW, col))
                .map(CellReference::goodHash)
                .collect(Collectors.toList());
        assertDistributionUniform(hashes, totalCount);
    }

    @Test
    public void sequentialRowFixedColumn() {
        int totalCount = 20_000;
        List<Integer> hashes = IntStream.range(0, totalCount)
                .mapToObj(row -> CellReference.of(TABLE, PtBytes.toBytes(row), COL))
                .map(CellReference::goodHash)
                .collect(Collectors.toList());
        assertDistributionUniform(hashes, totalCount);
    }

    @Test
    public void randomRowFixedColumn() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int totalCount = 50_000;
        List<byte[]> rows = IntStream.range(0, totalCount)
                .mapToObj(_ignore -> new byte[random.nextInt(1, 20)])
                .collect(Collectors.toList());
        rows.forEach(random::nextBytes);
        List<Integer> hashes = rows.stream()
                .map(row -> CellReference.of(TABLE, row, COL))
                .map(CellReference::goodHash)
                .collect(Collectors.toList());
        assertDistributionUniform(hashes, totalCount);
    }

    @Test
    public void matchingRowAndColumn() {
        int totalCount = 20_000;
        List<Integer> hashes = IntStream.range(0, totalCount)
                .mapToObj(PtBytes::toBytes)
                .map(bytes -> CellReference.of(TABLE, bytes, bytes))
                .map(CellReference::goodHash)
                .collect(Collectors.toList());
        assertDistributionUniform(hashes, totalCount);
    }

    private void assertDistributionUniform(List<Integer> hashes, int totalEntries) {
        assertDistributionUniformForBuckets(hashes, 2, totalEntries);
        assertDistributionUniformForBuckets(hashes, 3, totalEntries);
        assertDistributionUniformForBuckets(hashes, 5, totalEntries);
        assertDistributionUniformForBuckets(hashes, 7, totalEntries);
        assertDistributionUniformForBuckets(hashes, 11, totalEntries);
        assertDistributionUniformForBuckets(hashes, 32, totalEntries);
    }

    private void assertDistributionUniformForBuckets(List<Integer> hashes, int buckets, int totalEntries) {
        Map<Integer, Long> distribution = hashes.stream()
                .collect(Collectors.groupingBy(hash -> IntMath.mod(hash, buckets), Collectors.counting()));
        assertThat(distribution.size()).isEqualTo(buckets);
        distribution.values().forEach(count -> assertThat((double) count)
                .isCloseTo(totalEntries / (double) buckets, Percentage.withPercentage(15)));
    }
}
