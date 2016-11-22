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
package com.palantir.atlasdb.jepsen;

import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

public final class ClojureObjectUtils {
    private ClojureObjectUtils() {
    }

    public static boolean mapContainsKeywordNamed(PersistentArrayMap map, String keyName) {
        for (Object keyObject : map.keySet()) {
            if (isKeywordNamed(keyName, keyObject)) {
                return true;
            }
        }
        return false;
    }

    public static <T> T getFromKeywordNamed(PersistentArrayMap map, String keyName, Class<T> clazz) {
        for (Object keyObject : map.keySet()) {
            if (isKeywordNamed(keyName, keyObject)) {
                return clazz.cast(map.get(keyObject));
            }
        }
        return null;
    }

    public static <T extends Enum<T>> T getEnumFromKeywordNamed(PersistentArrayMap map, String keyName,
            Class<T> enumType) {
        Keyword keyword = ClojureObjectUtils.getFromKeywordNamed(map, keyName, Keyword.class);
        String keywordName = keyword.getName().replace('-', '_');
        return Enum.valueOf(enumType, keywordName.toUpperCase());
    }

    public static boolean isKeywordNamed(String keyName, Object keyObject) {
        return isKeyword(keyObject) && keywordName(keyObject).equals(keyName);
    }

    public static String keywordName(Object keyObject) {
        return ((Keyword) keyObject).getName();
    }

    public static boolean isKeyword(Object keyObject) {
        return keyObject instanceof Keyword;
    }
}
