/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.cassandra.multinode;

import com.palantir.atlasdb.containers.ThreeNodeCassandraResource;
import com.palantir.atlasdb.keyvalue.impl.AbstractMultiCasTestV2;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;

@Order(1) // No node is down.
public class CassandraMultiNodeMultiCasTest extends AbstractMultiCasTestV2 {
    @RegisterExtension
    public static final ThreeNodeCassandraResource CASSANDRA = new ThreeNodeCassandraResource();

    public CassandraMultiNodeMultiCasTest() {
        super(CASSANDRA);
    }
}
