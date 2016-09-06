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
package com.palantir.nexus.db.sql;

import org.apache.commons.lang3.StringUtils;

// TODO: This code was copied from papaya's TextUtils class, which will be moved to an internal product.
// If this project is also moved, then the duplication should be fixed.
public final class TextUtils {
    private TextUtils() {
        // Utility class
    }

    /**
     *  Makes a list of n copies of the given string, separated by the given delimiter.
     */
    public static String makeStringList(String str, String delimiter, int count) {
        return StringUtils.repeat(str, delimiter, count);
    }

    public static String removeAllWhitespace(String text) {
        return StringUtils.deleteWhitespace(text);
    }

}
