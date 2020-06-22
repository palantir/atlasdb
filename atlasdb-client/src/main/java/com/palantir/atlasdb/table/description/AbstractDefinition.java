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
package com.palantir.atlasdb.table.description;

import com.google.common.collect.ImmutableSet;
import com.google.common.math.IntMath;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.logsafe.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractDefinition {
    private static final Logger log = LoggerFactory.getLogger(AbstractDefinition.class);
    private static final ImmutableSet<ValueType> CRITICAL_ROW_TYPES = ImmutableSet.of(
            ValueType.VAR_LONG,
            ValueType.VAR_SIGNED_LONG,
            ValueType.VAR_STRING,
            ValueType.SIZED_BLOB);

    CachePriority cachePriority = CachePriority.WARM;
    ConflictHandler conflictHandler = defaultConflictHandler();
    SweepStrategy sweepStrategy = SweepStrategy.CONSERVATIVE;
    boolean ignoreHotspottingChecks = false;
    boolean explicitCompressionRequested = false;
    int explicitCompressionBlockSizeKb = 0;
    boolean rangeScanAllowed = false;
    boolean negativeLookups = false;
    boolean appendHeavyAndReadLight = false;

    public void cachePriority(CachePriority priority) {
        this.cachePriority = priority;
    }

    public void conflictHandler(ConflictHandler handler) {
        conflictHandler = handler;
    }

    public void sweepStrategy(SweepStrategy strategy) {
        this.sweepStrategy = strategy;
    }

    public void ignoreHotspottingChecks() {
        ignoreHotspottingChecks = true;
    }

    public boolean shouldIgnoreHotspottingChecks() {
        return ignoreHotspottingChecks;
    }

    public void rangeScanAllowed() {
        rangeScanAllowed = true;
    }

    public boolean isRangeScanAllowed() {
        return rangeScanAllowed;
    }

    public boolean isExplicitCompressionRequested() {
        return explicitCompressionRequested;
    }

    public void explicitCompressionRequested() {
        explicitCompressionRequested = true;
    }

    public int getExplicitCompressionBlockSizeKB() {
        return explicitCompressionBlockSizeKb;
    }

    public void explicitCompressionBlockSizeKB(int blockSizeKB) {
        Preconditions.checkArgument(IntMath.isPowerOfTwo(blockSizeKB),
                "explicitCompressionBlockSizeKB must be a power of 2");
        explicitCompressionBlockSizeKb = blockSizeKB;
    }

    public void negativeLookups() {
        negativeLookups = true;
    }

    public boolean hasNegativeLookups() {
        return negativeLookups;
    }

    public void appendHeavyAndReadLight() {
        appendHeavyAndReadLight = true;
    }

    public boolean isAppendHeavyAndReadLight() {
        return appendHeavyAndReadLight;
    }

    protected abstract ConflictHandler defaultConflictHandler();

    void validateFirstRowComp(NameComponentDescription comp) {
        if (!ignoreHotspottingChecks && CRITICAL_ROW_TYPES.contains(comp.getType())) {
            throw new IllegalStateException(String.format(
                    "First row component %s of type %s will likely cause hot-spotting with the partitioner in "
                            + "Cassandra. This is caused by the structure of variable-sized types which will state "
                            + "their length prior to their value resulting in them being partitioned predominantly by "
                            + "the LENGTH of the values which is likely to be similar. If you anticipate never running "
                            + "on Cassandra or feel you can safely ignore this case (for instance, if this table will "
                            + "never be very large) then you should add ignoreHotspottingChecks() to the table schema. "
                            + "This is directed at the developer of this AtlasDB application, they may need to change "
                            + "their schema.",
                    comp.getComponentName(), comp.getType()));
        }
    }
}
