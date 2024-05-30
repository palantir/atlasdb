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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.palantir.logsafe.SafeArg;
import com.palantir.util.AssertUtils;
import com.palantir.util.VersionStrings;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;

public final class PostgresVersionCheck {
    static final String MIN_POSTGRES_VERSION = "9.6";
    private static final Supplier<Pattern> VERSION_PATTERN = Suppliers.memoize(() -> {
        // Matches: x, x.y, x.y.z, etc in group 1
        // Allows discarding an optional additional section in parens, for example:
        // 14.11 (Ubuntu 14.11-1.pgdg20.04+1)
        return Pattern.compile("^([\\.0-9]+)( \\([^\\)]+\\))?$");
    });

    private PostgresVersionCheck() {}

    @VisibleForTesting
    static Optional<String> extractValidPostgresVersion(String rawVersionString) {
        Matcher matcher = VERSION_PATTERN.get().matcher(rawVersionString);
        if (matcher.matches()) {
            return Optional.of(matcher.group(1));
        } else {
            return Optional.empty();
        }
    }

    public static void checkDatabaseVersion(String version, Logger log) {
        Optional<String> parsedVersion = extractValidPostgresVersion(version);
        if (parsedVersion.isPresent()) {
            log.info("Parsed Postgres version", SafeArg.of("parsed", parsedVersion.get()), SafeArg.of("raw", version));
            boolean greaterThanMinVersion =
                    VersionStrings.compareVersions(parsedVersion.get(), MIN_POSTGRES_VERSION) >= 0;
            if (VersionStrings.compareVersions(parsedVersion.get(), "9.5") >= 0
                    && VersionStrings.compareVersions(parsedVersion.get(), "9.5.2") < 0) {
                // N.B. This situation is bad. Do not just log a warning and assert - actually throw an error.
                throw new DbkvsVersionException(
                        "You are running Postgres " + version + ". Versions 9.5.0 and 9.5.1 contain a known bug "
                                + "that causes incorrect results to be returned for certain queries. "
                                + "Please update your Postgres distribution.");
            }
            AssertUtils.assertAndLog(
                    log,
                    greaterThanMinVersion,
                    "Your key value service currently uses version %s of postgres, parsed to %s."
                            + " The minimum supported version is %s."
                            + " If you absolutely need to use an older version of postgres,"
                            + " please contact Palantir support for assistance.",
                    version,
                    parsedVersion,
                    MIN_POSTGRES_VERSION);
        } else {
            AssertUtils.assertAndLog(
                    log,
                    false,
                    "Unable to parse a version from postgres."
                            + " Raw format string was %s. Please contact Palantir support for assistance."
                            + " Minimum allowed Postgres version is %s.",
                    version,
                    MIN_POSTGRES_VERSION);
        }
    }
}
