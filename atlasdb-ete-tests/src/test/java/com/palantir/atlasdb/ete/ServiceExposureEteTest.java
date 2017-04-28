/*
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.ete;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.IsNot;
import org.hamcrest.core.IsNull;

import org.junit.Test;

import com.palantir.timestamp.TimestampService;

public class ServiceExposureEteTest {
    @Test
    public void shouldExposeATimestampServer() {
        TimestampService timestampClient = EteSetup.createClientToAllNodes(TimestampService.class);

        MatcherAssert.assertThat(timestampClient.getFreshTimestamp(), CoreMatchers.is(IsNot.not(IsNull.nullValue())));
    }
}
