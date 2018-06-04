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
package com.palantir.atlasdb.table.description;

import java.util.List;

public interface RowNamePartitioner {
    /**
     * These tokens should be as evenly distributed as
     * possible to of numberRanges ranges.
     * <p>
     * If this keyspace covers the whole region, this will take numRanges tokens.  If this keyspace
     * covers just a smaller section, this may require numberRanges+1 tokens.
     * <p>
     * It is also possible to return less tokens is the case where this keyspace cannot be broken
     * up easily or there are just very few keys for this keyspace.  If this is the case then
     * {@link #isHotSpot()} should return true so these ranges may be handled specially so many
     * hotspots don't end up on the same node.
     *
     * @param numberRanges will usually be a power of 2 to get even partitions
     */
    List<byte[]> getPartitions(int numberRanges);

    boolean isHotSpot();

    List<RowNamePartitioner> compound(RowNamePartitioner next);
}
