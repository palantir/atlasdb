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

package com.palantir.lock.v2;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class LeadershipIdTest {
    private static final String SERIALIZED_LEADERSHIP_ID = "\"fc71247c-66a5-4f94-be24-3a2b00d29968\"";
    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void ensureBackCompat() throws Exception {
        LeadershipId leadershipId = mapper.readValue(SERIALIZED_LEADERSHIP_ID, LeadershipId.class);
        assertThat(mapper.writeValueAsString(leadershipId)).isEqualTo(SERIALIZED_LEADERSHIP_ID);
    }
}
