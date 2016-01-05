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
package com.palantir.atlasdb.keyvalue.cassandra.dht;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.dht.ByteOrderedPartitioner;

public class AtlasDbPartitioner extends ByteOrderedPartitioner {
    private final Random r = new SecureRandom();
    private final AtomicInteger indexCounter = new AtomicInteger(r.nextInt(6));

    @Override
    public BytesToken getRandomToken() {
        byte[] buffer = new byte[16];
        r.nextBytes(buffer);

        switch ((indexCounter.getAndIncrement() & Integer.MAX_VALUE) % 6) {
        case 0:
        case 2:
        case 4:
            // This is the standard case of having a positive fixed long.
            // This is a very common case.
            // This is used for little endian longs, sha256 hashes as well.
            buffer[0] |= 0x80; // high bit is one for positive longs.
            return new BytesToken(buffer);
        case 1:
            // This is the standard case of having a negative fixed long.
            // This is used for little endian longs, sha256 hashes as well.
            buffer[0] &= 0x7f;
            return new BytesToken(buffer);
        case 3:
            // This is the 1, fixlong case.
            // This handles the base realm, positive object id specifically.
            buffer[0] = 1;
            buffer[1] |= 0x80; // we sort postive after neg by doing id ^ Long.MIN_VALUE
            return new BytesToken(buffer);
        case 5:
            // This is the positive varlong case.
            // This handles realm_id which is a var_long in most tables.
            long randVal = r.nextLong();
            randVal &= Long.MAX_VALUE;
            byte[] bytes = encodeVarLong(randVal);
            System.arraycopy(bytes, 0, buffer, 0, bytes.length);
            return new BytesToken(buffer);
        default:
            throw new IllegalStateException();
        }
    }

    private static byte[] encodeVarLong(long value) {
        int size = computeRawVarint64Size(value);
        byte[] ret = new byte[size];
        encodeVarLongForSize(value, ret, size);
        return ret;
    }

    /**
     * There will be size-1 bits set before there is a zero. All the bits of
     * value will or-ed (|=) onto the the passed byte[].
     *
     * @param size must be <= 17 (but will most likely be 10 or 11 at most)
     */
    private static void encodeVarLongForSize(long value, byte[] ret, int size) {
        int end = 0;
        if (size > 8) {
            ret[0] = (byte) 0xff;
            end = 1;
            size -= 8;
        }
        ret[end] = (byte) ((0xff << (9 - size)) & 0xff);

        int index = ret.length;
        while (index-- > end) {
            ret[index] |= (byte) ((int) value & 0xff);
            value >>>= 8;
        }
    }

    private static int computeRawVarint64Size(final long value) {
        if ((value & (0xffffffffffffffffL << 7)) == 0)
            return 1;
        if ((value & (0xffffffffffffffffL << 14)) == 0)
            return 2;
        if ((value & (0xffffffffffffffffL << 21)) == 0)
            return 3;
        if ((value & (0xffffffffffffffffL << 28)) == 0)
            return 4;
        if ((value & (0xffffffffffffffffL << 35)) == 0)
            return 5;
        if ((value & (0xffffffffffffffffL << 42)) == 0)
            return 6;
        if ((value & (0xffffffffffffffffL << 49)) == 0)
            return 7;
        if ((value & (0xffffffffffffffffL << 56)) == 0)
            return 8;
        if ((value & (0xffffffffffffffffL << 63)) == 0)
            return 9;
        return 10;
    }
}
