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
package com.palantir.atlasdb.ete;

import java.util.List;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.containers.CassandraVersion;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        TodoEteTest.class,
        DropwizardEteTest.class
})
public class CassandraMultinodeTestSuite extends EteSetup {
    private static final List<String> CLIENTS = ImmutableList.of("ete1", "ete2", "ete3");

    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition(
            CassandraMultinodeTestSuite.class,
            "docker-compose.cassandra.yml",
            CLIENTS,
            getEnvironment());

    private static Map<String, String> getEnvironment() {
        CassandraVersion version = CassandraVersion.fromEnvironment();
        return ImmutableMap.of("CASSANDRA_VERSION", version.exactVersion());
    }
}
