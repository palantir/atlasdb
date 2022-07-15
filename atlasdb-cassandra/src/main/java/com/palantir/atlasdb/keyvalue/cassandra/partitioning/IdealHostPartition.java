/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.partitioning;

import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import java.util.List;
import org.immutables.value.Value;
import org.immutables.value.Value.Parameter;

/**
 * Hard set on exactly 3 AZs and RF = 3
 * */
@Value.Immutable
public interface IdealHostPartition {
    @Parameter
    List<CassandraServer> setA();

    @Parameter
    List<CassandraServer> setB();

    @Parameter
    List<CassandraServer> setC();
}
