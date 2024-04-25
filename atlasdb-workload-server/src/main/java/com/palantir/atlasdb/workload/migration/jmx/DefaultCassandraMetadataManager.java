/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.migration.jmx;

import com.palantir.cassandra.manager.core.metadata.Datacenter;
import java.util.List;

public class DefaultCassandraMetadataManager implements CassandraMetadataManager {
    @Override
    public List<Datacenter> getAllDatacenters() { // TODO: I got too lazy to do it via jmx.
        return List.of(Datacenter.of("DC1"), Datacenter.of("DC2"));
    }

    @Override
    public Datacenter sourceDatacenter() {
        return Datacenter.of("DC1");
    }
}
