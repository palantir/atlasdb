/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.junit.After;
import org.junit.Test;

import com.palantir.atlasdb.illiteracy.RowWatchResource;
import com.palantir.atlasdb.illiteracy.StringWrapper;

public class WatchEteTest {
    private static final String MILLION_CHARS = String.join("", Collections.nCopies(1_000_000, "a"));

    private RowWatchResource rowWatchResource = EteSetup.createClientToSingleNode(RowWatchResource.class);

    @After
    public void resetGetCount() {
        rowWatchResource.resetGetCount();
    }

    @Test
    public void doNotWatchIfNotInterested() {
        rowWatchResource.put("orange", StringWrapper.of("banana"));
        assertThat(rowWatchResource.get("orange")).isEqualTo("banana");
        for (int i = 0; i < 5; i++) {
            assertThat(rowWatchResource.get("orange")).isEqualTo("banana");
        }
        assertThat(rowWatchResource.getGetCount()).isEqualTo(6);
        rowWatchResource.put("orange", StringWrapper.of("chocolate"));
        assertThat(rowWatchResource.get("orange")).isEqualTo("chocolate");
        for (int i = 0; i < 5; i++) {
            assertThat(rowWatchResource.get("orange")).isEqualTo("chocolate");
        }
        assertThat(rowWatchResource.getGetCount()).isEqualTo(12);
    }

    @Test
    public void updateWhenValueChanges() {
        rowWatchResource.beginWatching("cat");
        rowWatchResource.put("cat", StringWrapper.of("banana"));
        assertThat(rowWatchResource.get("cat")).isEqualTo("banana");
        rowWatchResource.flushCache();
        for (int i = 0; i < 5; i++) {
            assertThat(rowWatchResource.get("cat")).isEqualTo("banana");
        }
        assertThat(rowWatchResource.getGetCount()).isEqualTo(1);
        rowWatchResource.put("cat", StringWrapper.of("chocolate"));
        assertThat(rowWatchResource.get("cat")).isEqualTo("chocolate");
        rowWatchResource.flushCache();
        for (int i = 0; i < 5; i++) {
            assertThat(rowWatchResource.get("cat")).isEqualTo("chocolate");
        }
        assertThat(rowWatchResource.getGetCount()).isEqualTo(2);
    }

    @Test
    public void watchesDoApplyToPrefixes() {
        // TODO (jkong): Should the cache be lazy? Eager? Unclear.
        rowWatchResource.beginWatchingPrefix("mono");
        rowWatchResource.put("monotony", StringWrapper.of("banana"));
        rowWatchResource.put("monorepo", StringWrapper.of("map"));
        assertThat(rowWatchResource.get("monotony")).isEqualTo("banana");
        rowWatchResource.flushCache();
        assertThat(rowWatchResource.get("monorepo")).isEqualTo("map");
        for (int i = 0; i < 5; i++) {
            assertThat(rowWatchResource.get("monotony")).isEqualTo("banana");
            assertThat(rowWatchResource.get("monorepo")).isEqualTo("map");
        }
        assertThat(rowWatchResource.getGetCount()).isEqualTo(1);
    }

    @Test
    public void choosesExplicitIfExplicitAvailable() {
        rowWatchResource.beginWatchingPrefix("car");
        rowWatchResource.beginWatching("cartography");

        rowWatchResource.put("carbohydrate", StringWrapper.of("banana"));
        rowWatchResource.put("cartography", StringWrapper.of("map"));

        // load up the cache. gets = 1
        assertThat(rowWatchResource.get("carbohydrate")).isEqualTo("banana");
        rowWatchResource.flushCache();

        // sadly we are not smart enough to skip this read!
        assertThat(rowWatchResource.get("cartography")).isEqualTo("map");

        for (int i = 0; i < 5; i++) {
            rowWatchResource.put("carbohydrate", StringWrapper.of("banane"));
            assertThat(rowWatchResource.get("cartography")).isEqualTo("map");
        }
        assertThat(rowWatchResource.getGetCount()).isEqualTo(2);
    }

    @Test
    public void choosesMostPrecisePrefixIfAvailable() {
        rowWatchResource.beginWatchingPrefix("minim");
        rowWatchResource.beginWatchingPrefix("minima");

        rowWatchResource.put("minimal", StringWrapper.of("minimal"));
        rowWatchResource.put("minime", StringWrapper.of("me!"));

        // load up the cache. gets = 2
        assertThat(rowWatchResource.get("minimal")).isEqualTo("minimal");
        assertThat(rowWatchResource.get("minime")).isEqualTo("me!");
        rowWatchResource.flushCache();

        for (int i = 0; i < 5; i++) {
            rowWatchResource.put("minime", StringWrapper.of("me!!"));
            assertThat(rowWatchResource.get("minimal")).isEqualTo("minimal");
        }
        assertThat(rowWatchResource.getGetCount()).isEqualTo(2);
    }

    @Test
    public void updatePrefixWhenValueChanges() {
        rowWatchResource.beginWatchingPrefix("transactional-databases-");

        rowWatchResource.put("transactional-databases-2", StringWrapper.of("not-atlasdb"));

        // load up the cache. gets = 1
        assertThat(rowWatchResource.get("transactional-databases-2")).isEqualTo("not-atlasdb");
        rowWatchResource.flushCache();

        for (int i = 0; i < 5; i++) {
            rowWatchResource.put("transactional-databases-2", StringWrapper.of("not-atlasdb"));
            assertThat(rowWatchResource.get("transactional-databases-2")).isEqualTo("not-atlasdb");
        }
        assertThat(rowWatchResource.getGetCount()).isEqualTo(6);
    }


    //    @Test
//    public void bigSlowValues() {
//        rowWatchResource.beginWatching("apple");
//        rowWatchResource.put("apple", StringWrapper.of(MILLION_CHARS));
//        assertThat(rowWatchResource.get("apple")).isEqualTo(MILLION_CHARS);
//        rowWatchResource.flushCache();
//        for (int i = 0; i < 100; i++) {
//            assertThat(rowWatchResource.get("apple")).isEqualTo(MILLION_CHARS);
//        }
//        assertThat(rowWatchResource.getGetCount()).isEqualTo(1);
//    }
//
//    @Test
//    public void bigSlowValues2() {
//        rowWatchResource.put("dewberry", StringWrapper.of(MILLION_CHARS));
//        assertThat(rowWatchResource.get("dewberry")).isEqualTo(MILLION_CHARS);
//        rowWatchResource.flushCache();
//        for (int i = 0; i < 100; i++) {
//            assertThat(rowWatchResource.get("dewberry")).isEqualTo(MILLION_CHARS);
//        }
//        assertThat(rowWatchResource.getGetCount()).isEqualTo(101);
//    }
}
