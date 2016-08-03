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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Arrays;

public class CassandraServerVersion {
    private final int majorVersion;
    private final int minorVersion;

    public CassandraServerVersion(String versionString) {
        String[] components = versionString.split("\\.");
        if (components.length != 3) {
            throw new UnsupportedOperationException(String.format("Illegal version of Thrift protocol detected; expected format '#.#.#', got '%s'", Arrays.toString(components)));
        }
        majorVersion = Integer.parseInt(components[0]);
        minorVersion = Integer.parseInt(components[1]);
    }

    // This corresponds to the version change in https://github.com/apache/cassandra/commit/8b0e1868e8cf813ddfc98d11448aa2ad363eccc1#diff-2fa34d46c5a51e59f77d866bbe7ca02aR55
    public boolean supportsCheckAndSet() {
        return majorVersion > 19 || (majorVersion == 19 && minorVersion >= 37);
    }
}
