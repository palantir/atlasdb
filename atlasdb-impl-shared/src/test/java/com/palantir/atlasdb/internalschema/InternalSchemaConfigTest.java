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

package com.palantir.atlasdb.internalschema;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import org.junit.Test;

// In these tests we confirm if config is buildable, so we're interested in exceptions (or lack thereof).
@SuppressWarnings("ResultOfMethodCallIgnored")
public class InternalSchemaConfigTest {
    @Test
    public void canCreateConfigWithRecognisedSchemaVersions() {
        for (int version : TransactionConstants.SUPPORTED_TRANSACTIONS_SCHEMA_VERSIONS) {
            assertThatCode(() -> ImmutableInternalSchemaConfig.builder()
                            .targetTransactionsSchemaVersion(version)
                            .build())
                    .doesNotThrowAnyException();
        }
    }

    @Test
    public void throwsIfCreatingConfigWithUnrecognizedSchemaVersions() {
        assertThatThrownBy(() -> ImmutableInternalSchemaConfig.builder()
                        .targetTransactionsSchemaVersion(Integer.MIN_VALUE)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unrecognised transactions schema version.");
    }
}
