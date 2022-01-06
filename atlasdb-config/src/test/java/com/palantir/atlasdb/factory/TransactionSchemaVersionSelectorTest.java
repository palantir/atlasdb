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

package com.palantir.atlasdb.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class TransactionSchemaVersionSelectorTest {
    private final AtomicReference<Optional<Integer>> userVersion = new AtomicReference<>(Optional.empty());
    private final TransactionSchemaVersionSelector consistentKvsBackedSelector = new TransactionSchemaVersionSelector(
            CheckAndSetCompatibility.supportedBuilder()
                    .consistentOnFailure(true)
                    .supportsDetailOnFailure(false)
                    .build(),
            userVersion::get);
    private final TransactionSchemaVersionSelector inconsistentKvsBackedSelector = new TransactionSchemaVersionSelector(
            CheckAndSetCompatibility.supportedBuilder()
                    .consistentOnFailure(false)
                    .supportsDetailOnFailure(true)
                    .build(),
            userVersion::get);

    @Test
    public void passesThroughEmptyForConsistentKeyValueServices() {
        assertThat(consistentKvsBackedSelector.get()).isEmpty();
    }

    @Test
    public void passesThroughUserChoiceForConsistentKeyValueServices() {
        TransactionConstants.SUPPORTED_TRANSACTIONS_SCHEMA_VERSIONS.forEach(version -> {
            userVersion.set(Optional.of(version));
            assertThat(consistentKvsBackedSelector.get()).hasValue(version);
        });
    }

    @Test
    public void recommendsTwoStageEncodingForEmptyAndInconsistentKeyValueServices() {
        assertThat(inconsistentKvsBackedSelector.get())
                .contains(TransactionConstants.TWO_STAGE_ENCODING_TRANSACTIONS_SCHEMA_VERSION);
    }

    @Test
    public void recommendsTwoStageEncodingForNonInconsistentSupportedAndInconsistentKeyValueServices() {
        Sets.difference(
                        TransactionConstants.SUPPORTED_TRANSACTIONS_SCHEMA_VERSIONS,
                        TransactionConstants.SUPPORTED_TRANSACTION_SCHEMA_VERSIONS_FOR_CAS_INCONSISTENT_KVSES)
                .forEach(version -> {
                    userVersion.set(Optional.of(version));
                    assertThat(inconsistentKvsBackedSelector.get())
                            .hasValue(TransactionConstants.TWO_STAGE_ENCODING_TRANSACTIONS_SCHEMA_VERSION);
                });
    }

    @Test
    public void passesThroughUserChoiceForInonsistentKeyValueServicesIfSupported() {
        TransactionConstants.SUPPORTED_TRANSACTION_SCHEMA_VERSIONS_FOR_CAS_INCONSISTENT_KVSES.forEach(version -> {
            userVersion.set(Optional.of(version));
            assertThat(inconsistentKvsBackedSelector.get()).hasValue(version);
        });
    }

    @Test
    public void throwsForKeyValueServicesNotSupportingCheckAndSet() {
        assertThatThrownBy(() ->
                        new TransactionSchemaVersionSelector(CheckAndSetCompatibility.unsupported(), userVersion::get))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining(
                        "Should not determine transaction schema versions on a KVS not supporting check" + " and set");
    }
}
