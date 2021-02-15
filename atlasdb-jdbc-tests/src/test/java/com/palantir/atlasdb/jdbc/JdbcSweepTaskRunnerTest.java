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
package com.palantir.atlasdb.jdbc;

import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.sweep.AbstractSweepTaskRunnerTest;
import org.junit.After;
import org.junit.ClassRule;

public class JdbcSweepTaskRunnerTest extends AbstractSweepTaskRunnerTest {
    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(JdbcTests::createEmptyKvs);

    public JdbcSweepTaskRunnerTest() {
        super(TRM, TRM);
    }

    // this is necessary because jdbc does not update metadata for existing tables on create table
    @After
    public void dropTestTable() {
        kvs.dropTable(TABLE_NAME);
    }
}
