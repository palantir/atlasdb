/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import org.junit.Before;
import org.junit.Test;

public class SweeperServiceImplIntegrationTest extends AbstractBackgroundSweeperIntegrationTest {
    private SweeperService sweeperService;

    @Before
    @Override
    public void setup() {
        super.setup();
        sweeperService = new SweeperServiceImpl(specificTableSweeper, sweepBatchConfigSource);
    }

    @Override
    @Test
    public void smokeTest() throws Exception {
        createTable(TABLE_1, SweepStrategy.CONSERVATIVE);
        putManyCells(TABLE_1, 100, 110);
        putManyCells(TABLE_1, 103, 113);
        putManyCells(TABLE_1, 105, 115);
        sweeperService.sweepTableFully(TABLE_1.getQualifiedName());
        verifyTableSwept(TABLE_1, 75, true);
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return new InMemoryKeyValueService(true);
    }
}
