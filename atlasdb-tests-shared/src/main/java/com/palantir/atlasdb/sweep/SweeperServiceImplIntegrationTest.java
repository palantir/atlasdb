/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;

public class SweeperServiceImplIntegrationTest extends AbstractBackgroundSweeperIntegrationTest {
    private SweeperService sweeperService;

    @Override
    @Before
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
        sweepTimestamp.set(150);
        sweeperService.sweepTableFully(TABLE_1.getQualifiedName());
        verifyTableSwept(TABLE_1, 75, true);
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return new InMemoryKeyValueService(true);
    }
}
