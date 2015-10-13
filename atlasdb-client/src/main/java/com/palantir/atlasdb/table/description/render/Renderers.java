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
package com.palantir.atlasdb.table.description.render;

import com.palantir.atlasdb.table.description.DefaultIndexMetadata;
import com.palantir.atlasdb.table.description.TableDefinition;

public class Renderers {
    private Renderers() {
        // cannot instantiate
    }

    public static String camelCase(String string) {
        return camelCase(string, false);
    }

    public static String CamelCase(String string) {
        return camelCase(string, true);
    }

    private static String camelCase(String string, boolean lastWasUnderscore) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            if (c != '_') {
                if (lastWasUnderscore) {
                    s.append(Character.toUpperCase(c));
                } else {
                    s.append(c);
                }
            }
            lastWasUnderscore = c == '_';
        }
        return s.toString();
    }

    static String lower_case(String string) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            if (Character.isUpperCase(c)) {
                s.append('_');
            }
            s.append(Character.toLowerCase(c));
        }
        return s.toString();
    }

    static String UPPER_CASE(String string) {
        return lower_case(string).toUpperCase();
    }

    static String getClassTableName(String rawTableName, TableDefinition table) {
        if (table.getGenericTableName() != null) {
            return table.getGenericTableName();
        } else if (table.getJavaTableName() != null) {
            return table.getJavaTableName();
        } else {
            return Renderers.CamelCase(rawTableName);
        }
    }

    static String getIndexTableName(DefaultIndexMetadata index) {
        if (index.getJavaIndexName() == null) {
            return Renderers.CamelCase(index.getIndexName());
        } else {
            return index.getJavaIndexName();
        }
    }
}
