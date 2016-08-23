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
package com.palantir.atlasdb.ete.dropwizard;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;

import com.palantir.atlasdb.ete.EteSetup;

public class CassandraNoLeaderDropwizardEteTest extends DropwizardEteTest {
    @ClassRule
<<<<<<< 7033b8fc57203bf309772ac48101c6126fb91d56
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition("cassandra-no-leader-dropwizard", "docker-compose.no-leader.cassandra.yml");
=======
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition("cassandra-no-leader", "docker-compose.no-leader.cassandra.yml");
>>>>>>> merge develop into perf cli branch (#820)
}
