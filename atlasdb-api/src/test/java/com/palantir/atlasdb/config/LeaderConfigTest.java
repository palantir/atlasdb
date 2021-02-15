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
package com.palantir.atlasdb.config;

import java.util.Collections;
import org.junit.Test;

@SuppressWarnings("CheckReturnValue")
public class LeaderConfigTest {
    @Test(expected = IllegalStateException.class)
    public void cannotCreateALeaderConfigWithNoLeaders() {
        ImmutableLeaderConfig.builder()
                .localServer("me")
                .leaders(Collections.emptySet())
                .quorumSize(0)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void cannotCreateALeaderConfigWithQuorumSizeNotBeingAMajorityOfTheLeaders() {
        ImmutableLeaderConfig.builder()
                .localServer("me")
                .addLeaders("not me", "me")
                .quorumSize(1)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void cannotCreateALeaderConfigWithQuorumSizeLargerThanTheAmountOfLeaders() {
        ImmutableLeaderConfig.builder()
                .localServer("me")
                .addLeaders("not me", "me")
                .quorumSize(3)
                .build();
    }
}
