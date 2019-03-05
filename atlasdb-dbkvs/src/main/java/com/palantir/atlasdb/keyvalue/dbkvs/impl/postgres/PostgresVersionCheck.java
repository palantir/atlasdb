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

import org.slf4j.Logger;

import com.palantir.util.VersionStrings;

public final class PostgresVersionCheck {
    static final String MIN_POSTGRES_VERSION = "9.5.2";

    private PostgresVersionCheck() {}

    public static void checkDatabaseVersion(String version, Logger log) {
        if (!version.matches("^[\\.0-9]+$") || VersionStrings.compareVersions(version, MIN_POSTGRES_VERSION) < 0) {
            log.error("Your key value service currently uses version {} of postgres."
                    + " The minimum supported version is {}."
                    + " If you absolutely need to use an older version of postgres,"
                    + " please contact Palantir support for assistance.", version, MIN_POSTGRES_VERSION);
        }
    }
}
