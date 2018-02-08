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
import com.palantir.atlasdb.schema.cleanup.ArbitraryCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.CleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.NullCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;

public interface StreamStoreCleanupMetadataVisitor extends CleanupMetadata.Visitor<OnCleanupTask> {
    Logger log = LoggerFactory.getLogger(StreamStoreCleanupMetadataVisitor.class);

    OnCleanupTask NO_OP_CLEANUP_TASK = (tx, cells) -> {
        // As the name suggests, this doesn't need to do anything.
        return false;
    };

    @Override
    default OnCleanupTask visit(NullCleanupMetadata cleanupMetadata) {
        return NO_OP_CLEANUP_TASK;
    }

    OnCleanupTask visit(StreamStoreCleanupMetadata cleanupMetadata);

    // Note: Arbitrary is unsupported, but I'm overriding this for logging and to provide clearer output
    @Override
    default OnCleanupTask visit(ArbitraryCleanupMetadata cleanupMetadata) {
        log.warn("Attempted to construct a follower from arbitrary cleanup metadata, which is unexpected!");
        throw new UnsupportedOperationException("This visitor doesn't support arbitrary cleanup metadata");
    }
}
