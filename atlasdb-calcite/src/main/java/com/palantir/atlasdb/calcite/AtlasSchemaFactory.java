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
package com.palantir.atlasdb.calcite;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.palantir.atlasdb.api.AtlasDbService;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.impl.AtlasDbServiceImpl;
import com.palantir.atlasdb.impl.TableMetadataCache;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.services.ServicesConfigModule;

public class AtlasSchemaFactory implements SchemaFactory {
    public static final String ATLAS_CONFIG_FILE_KEY = "configFile";

    private static AtlasDbServices services;

    @VisibleForTesting
    public static AtlasDbServices getLastKnownAtlasServices() {
        return services;
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        AtlasDbService service = connectToAtlas(new File((String) operand.get(ATLAS_CONFIG_FILE_KEY)));
        return AtlasSchema.create(service);
    }

    private AtlasDbService connectToAtlas(File configFile) {
        try {
            // TODO (bullman): this is a hack for testing that should be removed
            if (services == null) {
                AtlasDbConfig config = AtlasDbConfigs.load(configFile);
                ServicesConfigModule scm = ServicesConfigModule.create(config);
                AtlasDbServices services = DaggerAtlasDbServices.builder().servicesConfigModule(scm).build();
                this.services = services;
            }
            return new AtlasDbServiceImpl(
                    services.getKeyValueService(),
                    services.getTransactionManager(),
                    new TableMetadataCache(services.getKeyValueService()));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
