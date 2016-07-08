/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.performance.generators;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.Stream;

public class RandomByteBufferGenerator {

    private int streamLength;
    private int byteArraySize;
    private Random rand;

    private RandomByteBufferGenerator(Builder b) {
        this.rand = new Random(b.SEED);
        this.streamLength = b.STREAM_LENGTH;
        this.byteArraySize = b.ARRAY_SIZE;
    }

    public Stream<ByteBuffer> stream() {
        return Stream.generate(this::genBytes).limit(streamLength);
    }

    private ByteBuffer genBytes() {
        byte[] b = new byte[byteArraySize];
        rand.nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        // Default values (arbitrary).
        private long SEED = 237;
        private int STREAM_LENGTH = 100000;
        private int ARRAY_SIZE = 100;

        public Builder withSeed(long seed) {
            this.SEED = seed;
            return this;
        }

        public Builder withByteArraySize(int size) {
            this.ARRAY_SIZE = size;
            return this;
        }

        public Builder length(int streamLength) {
            this.STREAM_LENGTH = streamLength;
            return this;
        }

        public RandomByteBufferGenerator build() {
            return new RandomByteBufferGenerator(this);
        }
    }
}
