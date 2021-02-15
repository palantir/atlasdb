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
package com.palantir.atlasdb.versions;

import com.google.common.base.Suppliers;
import java.io.PrintStream;
import java.util.Optional;
import java.util.function.Supplier;

public final class AtlasDbVersion {
    public static final String REPORT_VERSION_PROPERTY = "atlasdb.report.version";
    public static final String REPORT_VERSION_DEFAULT = "false";
    public static final String VERSION_UNKNOWN_STRING = "VERSION UNKNOWN";

    private static volatile boolean versionPrinted = false;

    private static final Supplier<String> version = Suppliers.memoize(AtlasDbVersion::readVersion);

    private AtlasDbVersion() {
        // static class
    }

    public static String ensureVersionReported() {
        return ensureVersionReported(System.out);
    }

    public static String ensureVersionReported(PrintStream writer) {
        boolean shouldReportVersion =
                Boolean.parseBoolean(System.getProperty(REPORT_VERSION_PROPERTY, REPORT_VERSION_DEFAULT));
        if (!versionPrinted && shouldReportVersion) {
            writer.println("AtlasDB Version: " + version.get());
            versionPrinted = true;
        }
        return version.get();
    }

    public static String readVersion() {
        return Optional.ofNullable(AtlasDbVersion.class.getPackage().getImplementationVersion())
                .orElse(VERSION_UNKNOWN_STRING);
    }
}
