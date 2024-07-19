/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.encoding.PtBytes;

public final class CqlUtilities {
    public static final long CASSANDRA_TIMESTAMP = -1;

    private CqlUtilities() {
        // Utility class
    }

    public static String encodeCassandraHexString(String string) {
        return encodeCassandraHexBytes(PtBytes.toBytes(string));
    }

    public static String encodeCassandraHexBytes(byte[] bytes) {
        return "0x" + BaseEncoding.base16().upperCase().encode(bytes);
    }
}
