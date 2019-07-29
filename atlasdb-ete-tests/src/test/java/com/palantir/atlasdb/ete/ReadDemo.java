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

import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.illiteracy.RowWatchResource;
import com.palantir.atlasdb.illiteracy.RowWatchResource2;
import com.palantir.atlasdb.illiteracy.StringWrapper;

public class ReadDemo {
    private RowWatchResource rowWatchResource = EteSetup.createClientToSingleNode(RowWatchResource.class);
    private RowWatchResource2 differentResource = EteSetup.createClientToSingleNode(RowWatchResource2.class);

    @After
    public void resetGetCount() {
        rowWatchResource.resetGetCount();
    }

    @Test
    public void readWorkflow() {
        rowWatchResource.put("-tom", StringWrapper.of("blue"));
        rowWatchResource.put("-tim", StringWrapper.of("yellow"));
        rowWatchResource.put("-andrew", StringWrapper.of("green"));
        rowWatchResource.put("-jeremy", StringWrapper.of("red"));
        rowWatchResource.flushCache();

        Map<String, String> response = rowWatchResource.getRange("-a", "-r");
        assertThat(response).isEqualTo(ImmutableMap.of("-andrew", "green", "-jeremy", "red"));

        response = rowWatchResource.getRange("-c", "-r");
        assertThat(response).isEqualTo(ImmutableMap.of("-jeremy", "red"));

        response = rowWatchResource.getRange("-t", "-tz");
        assertThat(response).isEqualTo(ImmutableMap.of("-tom", "blue", "-tim", "yellow"));

        String stringAnswer = rowWatchResource.get("-tom");
        assertThat(stringAnswer).isEqualTo("blue");

        differentResource.put("-jeremy", StringWrapper.of("black"));

        response = rowWatchResource.getRange("-t", "-tz");
        assertThat(response).isEqualTo(ImmutableMap.of("-tom", "blue", "-tim", "yellow"));

        response = rowWatchResource.getRange("-c", "-u");
        assertThat(response).isEqualTo(ImmutableMap.of("-tom", "blue", "-jeremy", "black", "-tim", "yellow"));

        response = rowWatchResource.getRange("-b", "-u");
        assertThat(response).isEqualTo(ImmutableMap.of("-tom", "blue", "-jeremy", "black", "-tim", "yellow"));

        stringAnswer = rowWatchResource.get("-tom");
        assertThat(stringAnswer).isEqualTo("blue");

        differentResource.put("-tim", StringWrapper.of("white"));

        stringAnswer = rowWatchResource.get("-tom");
        assertThat(stringAnswer).isEqualTo("blue");
    }

    @Test
    public void watchedReadWorkflow() {
        rowWatchResource.beginWatchingPrefix("-");
        rowWatchResource.beginWatchingPrefix("-t");
        rowWatchResource.beginWatching("-tom");

        rowWatchResource.put("-tom", StringWrapper.of("blue"));
        rowWatchResource.put("-tim", StringWrapper.of("yellow"));
        rowWatchResource.put("-andrew", StringWrapper.of("green"));
        rowWatchResource.put("-jeremy", StringWrapper.of("red"));
        rowWatchResource.flushCache();

        Map<String, String> response = rowWatchResource.getRange("-a", "-r"); // miss
        assertThat(response).isEqualTo(ImmutableMap.of("-andrew", "green", "-jeremy", "red"));

        response = rowWatchResource.getRange("-c", "-r"); // hit (watch 1 valid)
        assertThat(response).isEqualTo(ImmutableMap.of("-jeremy", "red"));

        response = rowWatchResource.getRange("-t", "-tz"); // miss
        assertThat(response).isEqualTo(ImmutableMap.of("-tom", "blue", "-tim", "yellow"));

        String stringAnswer = rowWatchResource.get("-tom"); // miss
        assertThat(stringAnswer).isEqualTo("blue");

        differentResource.put("-jeremy", StringWrapper.of("black"));

        response = rowWatchResource.getRange("-t", "-tz"); // hit (watch 2 valid)
        assertThat(response).isEqualTo(ImmutableMap.of("-tom", "blue", "-tim", "yellow"));

        response = rowWatchResource.getRange("-c", "-u"); // miss (watch 1 invalid)
        assertThat(response).isEqualTo(ImmutableMap.of("-tom", "blue", "-jeremy", "black", "-tim", "yellow"));

        response = rowWatchResource.getRange("-b", "-u"); // hit (watch 1 updated, now valid)
        assertThat(response).isEqualTo(ImmutableMap.of("-tom", "blue", "-jeremy", "black", "-tim", "yellow"));

        stringAnswer = rowWatchResource.get("-tom"); // hit (watch 3 valid)
        assertThat(stringAnswer).isEqualTo("blue");

        differentResource.put("-tim", StringWrapper.of("white"));

        stringAnswer = rowWatchResource.get("-tom"); // hit (watch 3 still valid)
        assertThat(stringAnswer).isEqualTo("blue");
    }
}
