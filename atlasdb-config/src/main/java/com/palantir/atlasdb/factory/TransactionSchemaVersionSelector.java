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

import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Optional;
import java.util.function.Supplier;

public class TransactionSchemaVersionSelector implements Supplier<Optional<Integer>> {
    private static final SafeLogger log = SafeLoggerFactory.get(TransactionSchemaVersionSelector.class);
    private static final Optional<Integer> MIN_ACCEPTABLE_VERSION_FOR_INCONSISTENT_CAS_KVSES =
            TransactionConstants.SUPPORTED_TRANSACTION_SCHEMA_VERSIONS_FOR_CAS_INCONSISTENT_KVSES.stream()
                    .min(Integer::compareTo);

    private final CheckAndSetCompatibility compatibility;
    private final Supplier<Optional<Integer>> userProvidedExplicitSupplier;

    public TransactionSchemaVersionSelector(
            CheckAndSetCompatibility compatibility, Supplier<Optional<Integer>> userProvidedExplicitSupplier) {
        Preconditions.checkState(
                compatibility.supportsCheckAndSetOperations(),
                "Should not determine transaction schema versions on a KVS not supporting check and set");
        this.compatibility = compatibility;
        this.userProvidedExplicitSupplier = userProvidedExplicitSupplier;
    }

    @Override
    public Optional<Integer> get() {
        Optional<Integer> userValue = userProvidedExplicitSupplier.get();

        if (!compatibility.consistentOnFailure()) {
            return recommendVersionForInconsistentCasKeyValueServices(userValue);
        }
        return userValue;
    }

    private Optional<Integer> recommendVersionForInconsistentCasKeyValueServices(Optional<Integer> userValue) {
        if (userValue.isEmpty()) {
            log.info(
                    "User is using a KVS that may be inconsistent on failure, and has not specified a transaction"
                            + " schema version. Recommending a minimum version, to avoid said inconsistency.",
                    SafeArg.of("recommendedVersion", MIN_ACCEPTABLE_VERSION_FOR_INCONSISTENT_CAS_KVSES));
            return MIN_ACCEPTABLE_VERSION_FOR_INCONSISTENT_CAS_KVSES;
        }
        int userSpecifiedVersion = userValue.get();
        if (!TransactionConstants.SUPPORTED_TRANSACTION_SCHEMA_VERSIONS_FOR_CAS_INCONSISTENT_KVSES.contains(
                userSpecifiedVersion)) {
            log.warn(
                    "User is using a KVS that may be inconsistent on failure, and has explicitly specified a"
                            + " transaction schema version not known to handle such inconsistencies."
                            + " Recommending a minimum version, to avoid said inconsistency.",
                    SafeArg.of("userSpecifiedVersion", userSpecifiedVersion),
                    SafeArg.of("recommendedVersion", MIN_ACCEPTABLE_VERSION_FOR_INCONSISTENT_CAS_KVSES));
            return MIN_ACCEPTABLE_VERSION_FOR_INCONSISTENT_CAS_KVSES;
        }
        return userValue;
    }
}
