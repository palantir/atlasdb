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

import java.util.StringTokenizer;

// TODO: This code was copied from papaya's TextUtils class, which will be moved to an internal product.
// If this project is also moved, then the duplication should be fixed.
public class TextUtils {
    private TextUtils() {
        // Utility class
    }

    /**
     *  Makes a list of n copies of the given string, separated by the given delimiter.
     */
    public static String makeStringList(String str, String delimiter, int count) {
        if (count == 0) {
            return "";
        }
        StringBuilder buf = new StringBuilder(str.length() * count + delimiter.length() * (count - 1));

        for (int i = 0; i < count - 1; i++) {
            buf.append(str);
            buf.append(delimiter);
        }
        buf.append(str);

        return buf.toString();
    }

    public static String removeAllWhitespace(String text) {
        return replaceAllWhitespace(text, "");
    }

    private static String replaceAllWhitespace(String text, String newSeparator) {
        StringBuilder sb = new StringBuilder();
        StringTokenizer st = new StringTokenizer(text);

        if (st.hasMoreTokens()) {
            sb.append(st.nextToken());
        }
        while (st.hasMoreTokens()) {
            sb.append(newSeparator);
            sb.append(st.nextToken());
        }

        return sb.toString();
    }
}
