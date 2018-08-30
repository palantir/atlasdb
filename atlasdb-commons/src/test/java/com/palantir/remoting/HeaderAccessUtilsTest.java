/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.remoting;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.common.remoting.HeaderAccessUtils;

public class HeaderAccessUtilsTest {
    private static final String FOO = "foo";
    private static final String KEY_1 = "variables";
    private static final String KEY_2 = "unix";
    private static final String KEY_3 = "haltingProblemProofs";
    private static final ImmutableList<String> VALUE_1 = ImmutableList.of(FOO, "bar", "baz");
    private static final ImmutableList<String> VALUE_2 = ImmutableList.of("ls", "du", "cut");
    private static final ImmutableList<String> VALUE_3 = ImmutableList.of();

    private static final Map<String, Collection<String>> HEADERS = ImmutableMap.<String, Collection<String>>builder()
            .put(KEY_1, VALUE_1)
            .put(KEY_2, VALUE_2)
            .put(KEY_3, VALUE_3)
            .build();

    @Test
    public void caseInsensitiveGetReturnsNullIfNoKeyMatches() {
        assertCaseInsensitiveGet("Diffie-Hellman", ImmutableList.<String>of());
    }

    @Test
    public void caseInsensitiveGetIgnoresCaseOnKeys() {
        assertCaseInsensitiveGet(KEY_1, VALUE_1);
        assertCaseInsensitiveGet(KEY_1.toUpperCase(), VALUE_1);
    }

    @Test
    public void caseInsensitiveGetShortcircuits() {
        Map<String, Collection<String>> testMap = Maps.newLinkedHashMap();
        String additionalCommand = "ps ax | awk '{print $1}' | xargs kill -9";
        testMap.put(KEY_2, VALUE_2);
        testMap.put(KEY_2.toUpperCase(), ImmutableList.of(additionalCommand));
        assertEquals(VALUE_2, HeaderAccessUtils.shortcircuitingCaseInsensitiveGet(testMap, KEY_2.toUpperCase()));
    }

    private static void assertCaseInsensitiveGet(String key, Collection<String> expected) {
        assertEquals(expected, HeaderAccessUtils.shortcircuitingCaseInsensitiveGet(HEADERS, key));
    }
}
