/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.table.description;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.util.PersistableBoolean;
import org.junit.Test;

public class SweeperStrategyTest {
    /**
     * Conservative sweep used to be encoded as a {@link PersistableBoolean#TRUE}, and thorough as
     * {@link PersistableBoolean#FALSE}.
     */
    @Test
    public void backwardsCompatibility() {
        assertThat(SweeperStrategy.CONSERVATIVE.persistToBytes())
                .containsExactly(PersistableBoolean.of(true).persistToBytes());
        assertThat(SweeperStrategy.THOROUGH.persistToBytes())
                .containsExactly(PersistableBoolean.of(false).persistToBytes());
    }

    @Test
    public void canDeserialize() {
        for (SweeperStrategy strategy : SweeperStrategy.values()) {
            assertThat(SweeperStrategy.BYTES_HYDRATOR.hydrateFromBytes(strategy.persistToBytes()))
                    .isEqualTo(strategy);
        }
    }
}
