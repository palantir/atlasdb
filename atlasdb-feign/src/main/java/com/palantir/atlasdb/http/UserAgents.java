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
package com.palantir.atlasdb.http;

import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

public final class UserAgents {
    private static final String ATLASDB_CLIENT = "atlasdb";

    public static final String USER_AGENT_FORMAT = "%s-" + ATLASDB_CLIENT + " (%s)";
    public static final String DEFAULT_VALUE = "unknown";
    public static final String DEFAULT_USER_AGENT = fromStrings(DEFAULT_VALUE, DEFAULT_VALUE);

    private UserAgents() {
        // utility class
    }

    public static String fromStrings(String userAgent, String version) {
        return String.format(USER_AGENT_FORMAT, userAgent, version);
    }

    /**
     * Constructs a user agent from the {@link java.util.jar.Attributes.Name#IMPLEMENTATION_TITLE} and
     * {@link java.util.jar.Attributes.Name#IMPLEMENTATION_VERSION} of the package of the provided class.
     * Typically, these are extracted from {@code MANIFEST.MF} entries of the jar package containing the given class.
     * The default value for both properties is "{@value DEFAULT_VALUE}".
     *
     * @param clazz Class to construct the user agent for
     * @return a user agent string for the given class
     */
    public static String fromClass(Class<?> clazz) {
        Package classPackage = clazz.getPackage();
        return fromPackage(classPackage);
    }

    @VisibleForTesting
    static String fromPackage(Package classPackage) {
        String agent = Optional.ofNullable(classPackage.getImplementationTitle()).orElse(DEFAULT_VALUE);
        String version = Optional.ofNullable(classPackage.getImplementationVersion()).orElse(DEFAULT_VALUE);
        return fromStrings(agent, version);
    }
}
