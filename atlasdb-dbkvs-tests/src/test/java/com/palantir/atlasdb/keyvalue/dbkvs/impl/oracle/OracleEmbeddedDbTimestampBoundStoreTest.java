/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.timestamp.AbstractDbTimestampBoundStoreTestV2;
import com.palantir.timestamp.TimestampBoundStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DbKvsOracleExtension.class)
public class OracleEmbeddedDbTimestampBoundStoreTest extends AbstractDbTimestampBoundStoreTestV2 {

    private KeyValueService kvs;

    @AfterEach
    public void tearDown() {
        kvs.close();
    }

    @Override
    protected TimestampBoundStore createTimestampBoundStore() {
        kvs = DbKvsOracleExtension.createKvs();
        return InDbTimestampBoundStore.create(
                ((ConnectionManagerAwareDbKvs) kvs).getConnectionManager(),
                AtlasDbConstants.TIMESTAMP_TABLE,
                DbKvsOracleExtension.getKvsConfig().ddl().tablePrefix());
    }
}
