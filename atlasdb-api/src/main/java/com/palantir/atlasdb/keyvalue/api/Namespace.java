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
package com.palantir.atlasdb.keyvalue.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.benmanes.caffeine.cache.Interner;
import com.google.common.base.CharMatcher;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.commons.lang3.Validate;

public final class Namespace {
    private static final Interner<String> names = Interner.newStrongInterner();

    public static final Namespace EMPTY_NAMESPACE = new Namespace("");
    public static final Namespace DEFAULT_NAMESPACE = new Namespace("default");

    /**
     * Unchecked name pattern (no dots).
     * <p>
     * This will not protect you from creating namespace that are incompatible with your underlying datastore.
     * <p>
     * Use {@link Namespace#LOOSELY_CHECKED_NAME} or {@link Namespace#STRICTLY_CHECKED_NAME} if possible.
     */
    public static final Pattern UNCHECKED_NAME = Pattern.compile("^[^.\\s]+$");

    private static final CharMatcher UNCHECKED_CHAR_MATCHER =
            CharMatcher.is('.').or(CharMatcher.whitespace()).negate().precomputed();

    /**
     * Less restrictive name pattern (letters, numbers, underscores, and hyphens).
     * <p>
     * Use {@link Namespace#STRICTLY_CHECKED_NAME} if possible.
     */
    public static final Pattern LOOSELY_CHECKED_NAME = Pattern.compile("^[\\w-]+$");

    /**
     * Restrictive name pattern (letters, numbers, and non-initial single underscores).
     */
    public static final Pattern STRICTLY_CHECKED_NAME = Pattern.compile("^(?!.*__.*)[a-zA-Z0-9]\\w*$");

    private final String name;

    public static Namespace create(String name) {
        return create(name, STRICTLY_CHECKED_NAME);
    }

    @SuppressWarnings("ValidateConstantMessage") // https://github.com/palantir/gradle-baseline/pull/175
    public static Namespace create(String name, Pattern pattern) {
        checkName(name);
        Validate.isTrue(pattern.matcher(name).matches(), "'%s' does not match namespace pattern '%s'.", name, pattern);
        return new Namespace(name);
    }

    /**
     * Creates a namespace for the given name, validating that the name does not contain whitespace or dots..
     *This will not protect you from creating namespace that are incompatible with your underlying datastore.
     *      <p>
     *      Use {@link Namespace#LOOSELY_CHECKED_NAME} or {@link Namespace#STRICTLY_CHECKED_NAME} if possible.
     * @throws SafeIllegalArgumentException if name contains whitespace or dots (period)
     */
    static Namespace createUnchecked(String name) {
        checkName(name);
        return new Namespace(name);
    }

    private static void checkName(String name) {
        Preconditions.checkArgument(
                !name.isEmpty() && UNCHECKED_CHAR_MATCHER.matchesAllOf(name),
                "namespace cannot be empty, contain whitespace, or contain dots (atlas reserved)");
    }

    @JsonCreator
    private Namespace(@JsonProperty("name") String name) {
        this.name = names.intern(name);
    }

    @JsonIgnore
    public boolean isEmptyNamespace() {
        return this.equals(EMPTY_NAMESPACE);
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return 31 + Objects.hashCode(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Namespace other = (Namespace) obj;
        return Objects.equals(name, other.name);
    }

    @Override
    public String toString() {
        return "Namespace [name=" + name + "]";
    }
}
