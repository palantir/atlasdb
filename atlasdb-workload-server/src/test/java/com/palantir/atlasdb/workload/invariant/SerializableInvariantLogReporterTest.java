/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.invariant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.palantir.atlasdb.workload.transaction.witnessed.FullyWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransaction;
import java.util.List;
import org.junit.Test;

public class SerializableInvariantLogReporterTest {
    @Test
    public void invariantIsSerializable() {
        assertThat(SerializableInvariantLogReporter.INSTANCE.invariant()).isEqualTo(SerializableInvariant.INSTANCE);
    }

    @Test
    public void consumerDoesNotThrowWhenAcceptingViolations() {
        assertThatCode(() -> SerializableInvariantLogReporter.INSTANCE
                        .consumer()
                        .accept(List.of(InvalidWitnessedTransaction.of(
                                FullyWitnessedTransaction.builder()
                                        .startTimestamp(1)
                                        .build(),
                                List.of()))))
                .doesNotThrowAnyException();
    }

    @Test
    public void consumerDoesNotThrowWhenAcceptingNoViolations() {
        assertThatCode(() ->
                        SerializableInvariantLogReporter.INSTANCE.consumer().accept(List.of()))
                .doesNotThrowAnyException();
    }
}
