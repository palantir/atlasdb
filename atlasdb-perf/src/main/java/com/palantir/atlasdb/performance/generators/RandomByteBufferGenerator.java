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
