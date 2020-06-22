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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import com.palantir.util.AssertUtils;
import com.palantir.util.VersionStrings;
import org.slf4j.Logger;

public final class PostgresVersionCheck {
    static final String MIN_POSTGRES_VERSION = "9.6";

    private PostgresVersionCheck() {}

    public static void checkDatabaseVersion(String version, Logger log) {
        boolean checkPasses = version.matches("^[\\.0-9]+$")
                && VersionStrings.compareVersions(version, MIN_POSTGRES_VERSION) >= 0;
        if (VersionStrings.compareVersions(version, "9.5") >= 0
                && VersionStrings.compareVersions(version, "9.5.2") < 0) {
            // N.B. This situation is bad. Do not just log a warning and assert - actually throw an error.
            throw new DbkvsVersionException(
                    "You are running Postgres " + version + ". Versions 9.5.0 and 9.5.1 contain a known bug "
                            + "that causes incorrect results to be returned for certain queries. "
                            + "Please update your Postgres distribution.");
        }
        AssertUtils.assertAndLog(log, checkPasses, "Your key value service currently uses version %s of postgres."
                + " The minimum supported version is %s."
                + " If you absolutely need to use an older version of postgres,"
                + " please contact Palantir support for assistance.", version, MIN_POSTGRES_VERSION);
    }
}
