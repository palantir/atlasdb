/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.rocksdb;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.spi.AtlasDbServerEnvironment;
import com.palantir.atlasdb.spi.AtlasDbServicePlugin;

@JsonDeserialize(as = ImmutableRocksDbServicePlugin.class)
@JsonSerialize(as = ImmutableRocksDbServicePlugin.class)
@JsonTypeName(RocksDbKeyValueServiceConfig.TYPE)
@Value.Immutable
@AutoService(AtlasDbServicePlugin.class)
public abstract class RocksDbServicePlugin implements AtlasDbServicePlugin {
    @Override
    public String type() {
        return RocksDbKeyValueServiceConfig.TYPE;
    }

    @Override
    public void registerServices(AtlasDbServerEnvironment environment) {
        KeyValueService kvs = new RocksDbAtlasDbFactory().createRawKeyValueService(getConfig());
        kvs.initializeFromFreshInstance();
        RemotingKeyValueService.registerKeyValueWithEnvironment(kvs, environment);
    }

    public abstract RocksDbKeyValueServiceConfig getConfig();

}
