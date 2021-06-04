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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.sweep.AbstractSweepTaskRunnerTest;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers.CacheWarming;
import java.util.Arrays;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DbKvsPostgresSweepTaskRunnerTest extends AbstractSweepTaskRunnerTest {
    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(DbKvsPostgresTestSuite::createKvs);

    @Parameterized.Parameters(name = "ssmCacheWarming={0}")
    public static Iterable<CacheWarming> data() {
        return Arrays.asList(CacheWarming.values());
    }

    private final CacheWarming ssmCacheWarming;

    public DbKvsPostgresSweepTaskRunnerTest(CacheWarming ssmCacheWarming) {
        super(TRM, TRM);
        this.ssmCacheWarming = ssmCacheWarming;
    }

    @Override
    protected CacheWarming getSsmCacheWarming() {
        return ssmCacheWarming;
    }
}
