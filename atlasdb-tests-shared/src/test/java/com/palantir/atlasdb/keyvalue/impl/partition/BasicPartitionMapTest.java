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
package com.palantir.atlasdb.keyvalue.impl.partition;

import java.util.NavigableMap;

import com.palantir.atlasdb.keyvalue.partition.BasicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;

public class BasicPartitionMapTest extends AbstractPartitionMapTest {

    private PartitionMap tpm;

    @Override
    protected PartitionMap getPartitionMap(QuorumParameters qp,
                                           NavigableMap<byte[], KeyValueEndpoint> ring) {
        if (tpm == null) {
            tpm = BasicPartitionMap.create(qp, ring);
        }
        return tpm;
    }

}
