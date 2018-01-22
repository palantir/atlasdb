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

package com.palantir.atlasdb.cleaner.external;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.cleanup.ArbitraryCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.CleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.NullCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;

public class FollowerGeneratingCleanupMetadataVisitor implements CleanupMetadata.Visitor<Follower> {
    private static final Logger log = LoggerFactory.getLogger(FollowerGeneratingCleanupMetadataVisitor.class);

    private static final Follower NO_OP_FOLLOWER = (txMgr, tableRef, cells, transactionType) -> {
        // As the name suggests, this doesn't need to do anything.
    };

    private final TableReference tableToSweep;

    public FollowerGeneratingCleanupMetadataVisitor(TableReference tableToSweep) {
        this.tableToSweep = tableToSweep;
    }

    @Override
    public Follower visit(NullCleanupMetadata cleanupMetadata) {
        return NO_OP_FOLLOWER;
    }

    @Override
    public Follower visit(StreamStoreCleanupMetadata cleanupMetadata) {
        // TODO (jkong): Fill me in
        return null;
    }

    // Note: Arbitrary is unsupported, but I'm overriding this for logging and to provide clearer output
    @Override
    public Follower visit(ArbitraryCleanupMetadata cleanupMetadata) {
        log.warn("Attempted to construct a follower from arbitrary cleanup metadata, which is unexpected!");
        throw new UnsupportedOperationException("This visitor doesn't support arbitrary cleanup metadata");
    }
}
