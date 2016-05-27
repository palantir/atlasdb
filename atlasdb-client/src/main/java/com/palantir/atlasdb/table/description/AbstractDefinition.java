/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.table.description;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ExpirationStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.PartitionStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

abstract class AbstractDefinition {
    private static final Logger log = LoggerFactory.getLogger(AbstractDefinition.class);
    private static final ImmutableSet<ValueType> CRITICAL_ROW_TYPES = ImmutableSet.of(
            ValueType.VAR_LONG,
            ValueType.VAR_SIGNED_LONG,
            ValueType.VAR_STRING,
            ValueType.SIZED_BLOB);

    CachePriority cachePriority = CachePriority.WARM;
    PartitionStrategy partitionStrategy = PartitionStrategy.ORDERED;
    ConflictHandler conflictHandler = defaultConflictHandler();
    SweepStrategy sweepStrategy = SweepStrategy.CONSERVATIVE;
    ExpirationStrategy expirationStrategy = ExpirationStrategy.NEVER;
    boolean ignoreHotspottingChecks = false;
    boolean explicitCompressionRequested = false;
    int explicitCompressionBlockSizeKB = 0;
    boolean rangeScanAllowed = false;
    boolean negativeLookups = false;
    boolean appendHeavyAndReadLight = false;

    public void cachePriority(CachePriority priority) {
        this.cachePriority = priority;
    }

    public void partitionStrategy(PartitionStrategy strat) {
        partitionStrategy = strat;
    }

    public void conflictHandler(ConflictHandler handler) {
        conflictHandler = handler;
    }

    public void expirationStrategy(ExpirationStrategy strategy) {
        expirationStrategy = strategy;
    }

    public ExpirationStrategy getExpirationStrategy() {
        return expirationStrategy;
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

    public boolean isExplicitCompressionRequested(){
        return explicitCompressionRequested;
    }

    public void explicitCompressionRequested() {
        explicitCompressionRequested = true;
    }

    public int getExplicitCompressionBlockSizeKB() {
        return explicitCompressionBlockSizeKB;
    }

    public void explicitCompressionBlockSizeKB(int blockSizeKB) {
        explicitCompressionBlockSizeKB = blockSizeKB;
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
        if (ignoreHotspottingChecks) {
            return;
        }

        if (CRITICAL_ROW_TYPES.contains(comp.getType())) {
            log.error(
                "First row component %s of type %s will likely cause hot-spotting with the partitioner in Cassandra. " +
                "If you anticipate never running on Cassandra or feel you can safely ignore this case " +
                "(for instance, if this table will never be very large) " +
                "then this error can be safely ignored by adding ignoreHotspottingChecks() to the table schema. " +
                "In a future release atlas will fail to start if there are hotspotting issues that have not been ignored. " +
                "(This error is directed at the developer of this atlas application, " +
                "who should be informed that they need to change their schema)",
                comp.getComponentName(), comp.getType());
        }
    }
}

