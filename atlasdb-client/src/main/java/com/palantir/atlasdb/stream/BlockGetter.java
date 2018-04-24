/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.stream;

import java.io.OutputStream;

/**
 * An interface intended for creating a way to fill some OutputStream. Used in {@link BlockConsumingInputStream}.
 */
public interface BlockGetter {
    /**
     * Fills the OutputStream with data from some source.
     * The source should be divisible into an ordered set of blocks.
     * The behaviour when too many blocks are requested is unspecified (in practice, an exception should be thrown).
     *
     * @param firstBlock the first block (0-indexed) to put into the OutputStream.
     * @param numBlocks the number of blocks to put into the OutputStream.
     * @param destination the OutputStream to fill with blocks.
     */
    void get(long firstBlock, long numBlocks, OutputStream destination);

    /**
     * Returns the expected length of a block of data.
     * @return expected length in bytes
     */
    int expectedBlockLength();
}
