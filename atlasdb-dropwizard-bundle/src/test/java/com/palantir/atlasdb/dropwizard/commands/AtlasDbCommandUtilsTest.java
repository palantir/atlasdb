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
package com.palantir.atlasdb.dropwizard.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

public class AtlasDbCommandUtilsTest {
    private static final String LOCAL_SERVER_NAME = "Local Server";
    private static final AtlasDbConfig MINIMAL_LEADER_CONFIG = ImmutableAtlasDbConfig.builder()
            .leader(ImmutableLeaderConfig.builder()
                    .quorumSize(1)
                    .addLeaders(LOCAL_SERVER_NAME)
                    .localServer(LOCAL_SERVER_NAME)
                    .build())
            .keyValueService(mock(KeyValueServiceConfig.class))
            .build();
    private static final AtlasDbConfig MINIMAL_EMBEDDED_CONFIG = ImmutableAtlasDbConfig.builder()
            .keyValueService(mock(KeyValueServiceConfig.class))
            .build();

    @Test
    public void leaderBlockNoLongerExistsAfterConvertingConfig() {
        AtlasDbConfig clientConfig = AtlasDbCommandUtils.convertServerConfigToClientConfig(MINIMAL_LEADER_CONFIG);

        assertThat(clientConfig.leader().isPresent()).isFalse();
    }

    @Test
    public void timestampBlockExistsAfterConvertingConfig() {
        AtlasDbConfig clientConfig = AtlasDbCommandUtils.convertServerConfigToClientConfig(MINIMAL_LEADER_CONFIG);

        assertThat(clientConfig.timestamp().isPresent()).isTrue();
    }

    @Test
    public void timestampBlockContainsLeadersAfterConvertingConfig() {
        AtlasDbConfig clientConfig = AtlasDbCommandUtils.convertServerConfigToClientConfig(MINIMAL_LEADER_CONFIG);

        assertThat(clientConfig.timestamp().get().servers()).containsExactly(LOCAL_SERVER_NAME);
    }

    @Test
    public void lockBlockExistsAfterConvertingConfig() {
        AtlasDbConfig clientConfig = AtlasDbCommandUtils.convertServerConfigToClientConfig(MINIMAL_LEADER_CONFIG);

        assertThat(clientConfig.lock().get().servers()).containsExactly(LOCAL_SERVER_NAME);
    }

    @Test(expected = IllegalArgumentException.class)
    public void conversionFailsWhenUsingEmbeddedServerConfig() {
        AtlasDbCommandUtils.convertServerConfigToClientConfig(MINIMAL_EMBEDDED_CONFIG);
    }

    @Test
    public void argumentsWithoutTwoHyphensAtTheBeginningAreFilteredOut() {
        List<String> gatheredArgs = AtlasDbCommandUtils.gatherPassedInArguments(
                ImmutableMap.of("unrelated-arg", "some-value"));

        assertThat(gatheredArgs).isEmpty();
    }

    @Test
    public void argumentsWhichHaveNullValuesAreFilteredOut() {
        Map<String, Object> args = new HashMap<>();
        args.put("--null-arg", null);

        List<String> gatheredArgs = AtlasDbCommandUtils.gatherPassedInArguments(args);

        assertThat(gatheredArgs).isEmpty();
    }

    @Test
    public void argumentsWhichDontHaveNullValuesAreKept() {
        List<String> gatheredArgs = AtlasDbCommandUtils.gatherPassedInArguments(
                ImmutableMap.of("--non-null-arg", "123"));

        assertThat(gatheredArgs).containsExactly("--non-null-arg", "123");
    }

    @Test
    public void argumentsWhichAreListsAreInlined() {
        List<String> gatheredArgs = AtlasDbCommandUtils.gatherPassedInArguments(
                ImmutableMap.of("--list-arg", ImmutableList.of("123", "456")));

        assertThat(gatheredArgs).containsExactly("--list-arg", "123", "456");
    }

    @Test
    public void argumentsWithTheZeroArityStringHaveOnlyTheKeyKept() {
        List<String> gatheredArgs = AtlasDbCommandUtils.gatherPassedInArguments(
                ImmutableMap.of("--zero-arity-arg", AtlasDbCommandUtils.ZERO_ARITY_ARG_CONSTANT));

        assertThat(gatheredArgs).containsExactly("--zero-arity-arg");
    }
}
