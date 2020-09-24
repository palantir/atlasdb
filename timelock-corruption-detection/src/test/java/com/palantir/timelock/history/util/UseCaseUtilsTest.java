/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.history.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.timelock.history.HistoryQuery;

public class UseCaseUtilsTest {
    @Test
    public void throwsOnInvalidFormatUseCases() {
        assertHasInvalidFormat("abcdefg");
        assertHasInvalidFormat("");
    }

    @Test
    public void parsesSimpleUseCases() {
        assertThat(UseCaseUtils.getPaxosUseCasePrefix("tom!acceptor")).isEqualTo("tom");
        assertThat(UseCaseUtils.getPaxosUseCasePrefix("tom!learner")).isEqualTo("tom");
    }

    @Test
    public void parsesUseCasesWithMultipleDelimiters() {
        assertThat(UseCaseUtils.getPaxosUseCasePrefix("a!b!c!d")).isEqualTo("a!b!c");
        assertThat(UseCaseUtils.getPaxosUseCasePrefix("!!!!!!!")).isEqualTo("!!!!!!");
    }

    @Test
    public void blah() throws JsonProcessingException {
        ImmutableNamespaceAndUseCase namespaceAndUseCase = ImmutableNamespaceAndUseCase.of(Client.of("gatekeeper"), "client");
        List<HistoryQuery> hs = ImmutableList.of(HistoryQuery.of(
                namespaceAndUseCase, -1));
        ObjectMapper om = new ObjectMapper();
        System.out.println(om.writeValueAsString(hs));
        System.out.println(om.writeValueAsString(namespaceAndUseCase));

    }

    private void assertHasInvalidFormat(String candidateUseCase) {
        assertThatThrownBy(() -> UseCaseUtils.getPaxosUseCasePrefix(candidateUseCase))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Unexpected use case format");
    }
}
