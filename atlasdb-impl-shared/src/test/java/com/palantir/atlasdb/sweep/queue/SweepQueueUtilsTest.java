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
package com.palantir.atlasdb.sweep.queue;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class SweepQueueUtilsTest {

    @Test
    public void fineToCoarseIsCorrect() {
        long prime = 674506111L;
        Assertions.assertThat(SweepQueueUtils.partitionFineToCoarse(SweepQueueUtils.tsPartitionFine(prime)))
                .isEqualTo(SweepQueueUtils.tsPartitionCoarse(prime));
    }
}
