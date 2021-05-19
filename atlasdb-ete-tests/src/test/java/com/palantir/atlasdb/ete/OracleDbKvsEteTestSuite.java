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

package com.palantir.atlasdb.ete;

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(TodoEteTest.class)
public class OracleDbKvsEteTestSuite extends EteSetup {
    private static final ImmutableList<String> CLIENTS = ImmutableList.of("ete1", "ete2", "ete3");

    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition(
            OracleDbKvsEteTestSuite.class,
            "docker-compose.timelock.oracle.yml",
            CLIENTS,
            Duration.ofMinutes(10));
}
