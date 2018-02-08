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

package com.palantir.atlasdb.sweep.external;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;
import com.palantir.logsafe.SafeArg;

public class GenericStreamStoreIndexCleanupVisitor implements StreamStoreCleanupMetadataVisitor {
    private static final Logger log = LoggerFactory.getLogger(GenericStreamStoreIndexCleanupVisitor.class);

    private final TableReference tableToSweep;

    public GenericStreamStoreIndexCleanupVisitor(TableReference tableToSweep) {
        this.tableToSweep = tableToSweep;
    }

    @Override
    public OnCleanupTask visit(StreamStoreCleanupMetadata cleanupMetadata) {
        log.info("Now creating cleanup task for table {}, which has cleanup metadata {}",
                LoggingArgs.tableRef(tableToSweep),
                SafeArg.of("cleanupMetadata", cleanupMetadata));
        return GenericStreamStoreCleanupTask.createForIndexTable(tableToSweep, cleanupMetadata);
    }
}
