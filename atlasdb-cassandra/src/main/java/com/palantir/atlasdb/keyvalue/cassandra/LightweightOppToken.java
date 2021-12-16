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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.datastax.driver.core.Token;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class LightweightOppToken implements Comparable<LightweightOppToken> {

    final byte[] bytes;

    public LightweightOppToken(byte[] bytes) {
        this.bytes = bytes;
    }

    public static LightweightOppToken of(Cell cell) {
        return new LightweightOppToken(cell.getRowName());
    }

    public static LightweightOppToken serialize(Token token) {
        ByteBuffer serializedToken = token.serialize(CassandraConstants.DEFAULT_PROTOCOL_VERSION);
        byte[] bytes = new byte[serializedToken.remaining()];
        serializedToken.get(bytes);
        return new LightweightOppToken(bytes);
    }

    @Override
    public int compareTo(LightweightOppToken other) {
        return UnsignedBytes.lexicographicalComparator().compare(this.bytes, other.bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LightweightOppToken that = (LightweightOppToken) obj;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return BaseEncoding.base16().encode(bytes);
    }
}
