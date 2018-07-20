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

package com.palantir.atlasdb.config;

import org.immutables.value.Value;

@Value.Immutable
public interface TransactionSchemaConfig {
    /**
     * Sets AtlasDB to use the {@link com.palantir.atlasdb.transaction.impl.TransactionConstants#TRANSACTION_TABLE_V2}
     * table for storing its mapping of future start to commit timestamps.
     *
     * Please note that this configuration change must only be deployed in a shut-down way, or severe data corruption
     * may result. For example, transactions written by nodes with the old configuration may become invisible to nodes
     * with the new configuration. In general, we do not guarantee any specific behaviour in this case.
     *
     * Once this option has been set to true, it must not be re-set to false.
     * If it was ever true and was subsequently re-set to false, AtlasDB exhibits undefined behaviour.
     */
    @Value.Default
    boolean useTransactions2TablePleaseContactAtlasDbTeamBeforeUsing();
}
