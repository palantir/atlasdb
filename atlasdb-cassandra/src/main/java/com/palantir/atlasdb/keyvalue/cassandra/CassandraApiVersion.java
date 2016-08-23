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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraApiVersion {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraApiVersion.class);

    private final String versionString;
    private final int majorVersion;
    private final int minorVersion;

    public CassandraApiVersion(String versionString) {
        this.versionString = versionString;
        String[] components = versionString.split("\\.");
        if (components.length != 3) {
            throw new UnsupportedOperationException(String.format(
                    "Illegal version of Thrift protocol detected; expected format '#.#.#', got '%s'",
                    Arrays.toString(components)));
        }
        majorVersion = Integer.parseInt(components[0]);
        minorVersion = Integer.parseInt(components[1]);
    }

    // This corresponds to the version change in
    // https://github.com/apache/cassandra/commit/8b0e1868e8cf8
    public boolean supportsCheckAndSet() {
        boolean supportsCheckAndSet = majorVersion > 19 || (majorVersion == 19 && minorVersion >= 37);

        String supportMessage = supportsCheckAndSet ? "supports check and set." : "does not support check and set.";

        LOGGER.info("Your cassandra api version ({}) " + supportMessage, versionString);

        return supportsCheckAndSet;
    }
<<<<<<< 7033b8fc57203bf309772ac48101c6126fb91d56

    @Override
    public String toString() {
        return versionString;
    }
=======
>>>>>>> merge develop into perf cli branch (#820)
}
