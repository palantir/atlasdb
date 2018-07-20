/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.service.v2;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.CoordinationServiceImpl;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.service.ImmutableTransactionSchemaMetadata;
import com.palantir.atlasdb.transaction.service.TransactionSchemaMetadata;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;

public class TransactionV2BoundUpdater {
    private static final Logger log = LoggerFactory.getLogger(TransactionV2BoundUpdater.class);

    // TODO (jkong): This should change for live migrations
    private static final long SHUTDOWN_VALID = -1;

    private final CoordinationService<TransactionSchemaMetadata> coordinationService;
    private final TimelockService timelockService;

    private TransactionV2BoundUpdater(
            CoordinationService<TransactionSchemaMetadata> coordinationService,
            TimelockService timelockService) {
        this.coordinationService = coordinationService;
        this.timelockService = timelockService;
    }

    public static TransactionV2BoundUpdater create(KeyValueService keyValueService, TimelockService timelockService) {
        CoordinationService<TransactionSchemaMetadata> coordinationService = CoordinationServiceImpl.create(
                keyValueService,
                TransactionSchemaMetadata.TRANSACTION_SCHEMA_METADATA,
                TransactionSchemaMetadata.class);
        return new TransactionV2BoundUpdater(coordinationService, timelockService);
    }

    public long getTransactions2LowerBound() {
        return coordinationService.get()
                .filter(metadata -> metadata.schemaVersion() == 2)
                .flatMap(TransactionSchemaMetadata::transactions2LowerBound)
                .orElseThrow(() -> new IllegalStateException(
                        "Attempted to getTransactions2LowerBound, but the schema wasn't known to be version 2."));
    }

    public void ensureSchemaIsAtLeastVersion2() {
        TransactionSchemaMetadata targetMetadata = getTargetMetadata();
        while (true) {
            Optional<TransactionSchemaMetadata> versionAndBound = coordinationService.get();
            if (!versionAndBound.isPresent()) {
                try {
                    coordinationService.putUnlessExists(targetMetadata);
                    log.info("Read no transaction schema metadata, updated to {}",
                            SafeArg.of("targetMetadata", targetMetadata));
                } catch (KeyAlreadyExistsException e) {
                    log.info("Attempted to update transaction schema metadata from empty to {}, but it was"
                            + " written to when we tried to do so.",
                            SafeArg.of("targetMetadata", targetMetadata),
                            e);
                }
            } else {
                // versionAndBound is present
                TransactionSchemaMetadata metadata = versionAndBound.get();
                if (metadata.schemaVersion() < 2) {
                    try {
                        coordinationService.checkAndSet(metadata, targetMetadata);
                        log.info("Read transaction schema metadata of {} and updated it to {}",
                                SafeArg.of("existingMetadata", metadata),
                                SafeArg.of("targetMetadata", targetMetadata));
                        return;
                    } catch (CheckAndSetException e) {
                        // Try again
                        log.info("Read transaction schema metadata of {} and tried to update it to {},"
                                        + " but someone else updated the metadata in the meantime.",
                                SafeArg.of("existingMetadata", metadata),
                                SafeArg.of("targetMetadata", targetMetadata),
                                e);
                    }
                } else {
                    // The schema version is high enough.
                    log.info("Read transaction schema metadata of {}, which is recent enough.",
                            SafeArg.of("existingMetadata", metadata));
                    return;
                }
            }
        }
    }

    private TransactionSchemaMetadata getTargetMetadata() {
        return ImmutableTransactionSchemaMetadata.builder()
                .schemaVersion(2)
                .validity(SHUTDOWN_VALID)
                .transactions2LowerBound(timelockService.getFreshTimestamp())
                .build();
    }

}
