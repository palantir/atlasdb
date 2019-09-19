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
package com.palantir.util;

import com.google.common.base.Splitter;
import java.util.Iterator;

public final class VersionStrings {
    private VersionStrings() {
        // utility class
    }

    public static int compareVersions(String v1, String v2) {
        Iterator<String> v1Iter = Splitter.on('.').trimResults().split(v1).iterator();
        Iterator<String> v2Iter = Splitter.on('.').trimResults().split(v2).iterator();
        while (v1Iter.hasNext() && v2Iter.hasNext()) {
            int v1Part = Integer.parseInt(v1Iter.next());
            int v2Part = Integer.parseInt(v2Iter.next());
            if (v1Part != v2Part) {
                return v1Part - v2Part;
            }
        }
        while (v1Iter.hasNext()) {
            if (Integer.parseInt(v1Iter.next()) != 0) {
                return 1;
            }
        }
        while (v2Iter.hasNext()) {
            if (Integer.parseInt(v2Iter.next()) != 0) {
                return -1;
            }
        }
        return 0;
    }
}
