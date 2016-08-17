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
package com.palantir.atlasdb.keyvalue.partition.endpoint;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;

/**
 * This class is used to represent an endpoint for the <code>PartitionedKeyValueService</code>.
 * Whenever the keyValueService throws a <code>VersionTooOldException</code>, the partitionMapService
 * shall be used to update local DynamicPartitionMap instance.
 *
 * @author htarasiuk
 *
 */
@JsonSerialize(as = SimpleKeyValueEndpoint.class)
@JsonDeserialize(as = SimpleKeyValueEndpoint.class)
public interface KeyValueEndpoint {
    KeyValueService keyValueService();
    PartitionMapService partitionMapService();
    String rack();

    // TODO: This should be replaced by a nicer solution eventually.
    void registerPartitionMapVersion(Supplier<Long> clientVersionSupplier);
}
