/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.schema;

import org.apache.commons.lang.Validate;

import com.google.common.base.Objects;
import com.google.common.base.Strings;

public final class Namespace {
    public static final Namespace EMPTY_NAMESPACE = new Namespace("");
    public static final Namespace DEFAULT_NAMESPACE = new Namespace("default");

    private final String name;

    public static Namespace create(String name) {
        Validate.isTrue(!Strings.isNullOrEmpty(name));
        Validate.isTrue(isNamespaceValid(name), "'%s' contains invalid characters, only letters, numbers, or non-initial/non-trailing single underscores are allowed.", name);
        return new Namespace(name);
    }

    Namespace(String name) {
        this.name = name;
    }

    public boolean isEmptyNamespace() {
        return this == EMPTY_NAMESPACE;
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
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
        return Objects.equal(name, other.name);
    }

    @Override
    public String toString() {
        return "Namespace [name=" + name + "]";
    }

    public static boolean isNamespaceValid(String namespace) {
        for (int i = 0; i < namespace.length() ; i++) {
            char c = namespace.charAt(i);
            if (!Character.isLetterOrDigit(c)) {
                // only underscores are additionally allowed
                if (c != '_') {
                    return false;
                }
                // underscores must be non-initial and non-trailing
                else if (i == 0 || i == namespace.length() - 1) {
                    return false;
                }
                // disallow double underscores
                else if (namespace.charAt(i + 1) == '_') {
                    return false;
                }
            }
        }
        return true;
    }
}
