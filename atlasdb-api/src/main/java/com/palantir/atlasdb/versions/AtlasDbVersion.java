/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.versions;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.CharStreams;

public final class AtlasDbVersion {
    public static final String REPORT_VERSION_PROPERTY = "atlasdb.report.version";
    public static final String REPORT_VERSION_DEFAULT = "false";
    public static final String VERSION_UNKNOWN_STRING = "VERSION UNKNOWN";
    public static final String VERSION_INFO_FILE = "/META-INF/atlasdb.version";

    private static volatile boolean versionPrinted = false;

    private static final Supplier<String> version = Suppliers.memoize(() -> readVersion());

    private AtlasDbVersion() {
        // static class
    }

    public static String ensureVersionReported() {
        return ensureVersionReported(System.out);
    }

    public static String ensureVersionReported(PrintStream writer) {
        boolean shouldReportVersion = Boolean.parseBoolean(System.getProperty(
                REPORT_VERSION_PROPERTY,
                REPORT_VERSION_DEFAULT));
        if (!versionPrinted && shouldReportVersion) {
            writer.println("AtlasDB Version: " + version.get());
            versionPrinted = true;
        }
        return version.get();
    }

    public static String readVersion() {
        try (InputStream is = AtlasDbVersion.class.getResourceAsStream(VERSION_INFO_FILE)) {
            if (is == null) {
                return VERSION_UNKNOWN_STRING;
            }
            return CharStreams.toString(new InputStreamReader(is, StandardCharsets.UTF_8)).trim();
        } catch (IOException e) {
            return VERSION_UNKNOWN_STRING;
        }
    }

}
