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
package com.palantir.atlasdb.keyvalue.cassandra.dht;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.ArrayUtils;

public class AtlasDbOrderedPartitioner extends ByteOrderedPartitioner {
    private final Random r = new SecureRandom();
    private final AtomicInteger indexCounter = new AtomicInteger(r.nextInt(6));
    public static final AtlasBytesToken MINIMUM = new AtlasBytesToken(ArrayUtils.EMPTY_BYTE_ARRAY);

    public static final AtlasDbOrderedPartitioner instance = new AtlasDbOrderedPartitioner();

    public static class AtlasBytesToken extends ByteOrderedPartitioner.BytesToken {
        private static final long serialVersionUID = 8285261694722952407L;

        public AtlasBytesToken(ByteBuffer token)
        {
            this(ByteBufferUtil.getArray(token));
        }

        public AtlasBytesToken(byte[] token) {
            super(token);
        }

        @Override
        public IPartitioner getPartitioner()
        {
            return instance;
        }
    }

    @Override
    public AtlasBytesToken getToken(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return MINIMUM;
        return new AtlasBytesToken(key);
    }


    @Override
    public AtlasBytesToken getMinimumToken()
    {
        return MINIMUM;
    }

    @Override
    public AtlasBytesToken midpoint(Token lt, Token rt)
    {
        byte[] leftTokenValue = (byte[]) lt.getTokenValue();
        byte[] rightTokenValue = (byte[]) rt.getTokenValue();

        int sigbytes = Math.max(leftTokenValue.length, rightTokenValue.length);
        BigInteger left = bigForBytes(leftTokenValue, sigbytes);
        BigInteger right = bigForBytes(rightTokenValue, sigbytes);

        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(left, right, 8*sigbytes);
        return new AtlasBytesToken(bytesForBig(midpair.left, sigbytes, midpair.right));
    }

    @Override
    public AtlasBytesToken getRandomToken() {
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
            return new AtlasBytesToken(buffer);
        case 1:
            // This is the standard case of having a negative fixed long.
            // This is used for little endian longs, sha256 hashes as well.
            buffer[0] &= 0x7f;
            return new AtlasBytesToken(buffer);
        case 3:
            // This is the 1, fixlong case.
            // This handles the base realm, positive object id specifically.
            buffer[0] = 1;
            buffer[1] |= 0x80; // we sort positive after neg by doing id ^ Long.MIN_VALUE
            return new AtlasBytesToken(buffer);
        case 5:
            // This is the positive varlong case.
            // This handles realm_id which is a var_long in most tables.
            long randVal = r.nextLong();
            randVal &= Long.MAX_VALUE;
            byte[] bytes = encodeVarLong(randVal);
            System.arraycopy(bytes, 0, buffer, 0, bytes.length);
            return new AtlasBytesToken(buffer);
        default:
            throw new IllegalStateException();
        }
    }

    /**
     * Get a billion FDEs off my back about their ownership percentages being wonky
     * because AbstractByteOrdererPartitioner assumes a uniform token distribution that we don't have.
     * Also should fix an ArrayIndexOutOfBoundsException that can happen while nodetool status-ing soon
     * after cluster startup.
     */
    @Override
    public Map<Token, Float> describeOwnership(final List<Token> sortedTokens) {
        return Maps.asMap(ImmutableSet.copyOf(sortedTokens), token -> {
            if (sortedTokens.size() > 0) {
                return 1f / sortedTokens.size();
            } else {
                return 0f;
            }
        });
    }

    private static byte[] encodeVarLong(long value) {
        int size = computeRawVarint64Size(value);
        byte[] ret = new byte[size];
        encodeVarLongForSize(value, ret, size);
        return ret;
    }

    /**
     * There will be size-1 bits set before there is a zero. All the bits of
     * value will be or-ed (|=) onto the the passed byte[].
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

    private final Token.TokenFactory tokenFactory = new Token.TokenFactory() {
        @Override
        public ByteBuffer toByteArray(Token token)
        {
            AtlasBytesToken bytesToken = (AtlasBytesToken) token;
            return ByteBuffer.wrap((byte[]) bytesToken.getTokenValue());
        }

        @Override
        public Token fromByteArray(ByteBuffer bytes)
        {
            return new AtlasBytesToken(bytes);
        }

        @Override
        public String toString(Token token)
        {
            AtlasBytesToken bytesToken = (AtlasBytesToken) token;
            return Hex.bytesToHex((byte[]) bytesToken.getTokenValue());
        }

        @Override
        public void validate(String token) throws ConfigurationException
        {
            try
            {
                if (token.length() % 2 == 1)
                    token = "0" + token;
                Hex.hexToBytes(token);
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException("Token " + token + " contains non-hex digits");
            }
        }

        @Override
        public Token fromString(String string)
        {
            if (string.length() % 2 == 1)
                string = "0" + string;
            return new AtlasBytesToken(Hex.hexToBytes(string));
        }
    };

    @Override
    public Token.TokenFactory getTokenFactory()
    {
        return tokenFactory;
    }
}
