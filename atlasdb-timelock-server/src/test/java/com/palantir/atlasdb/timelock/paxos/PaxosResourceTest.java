/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PaxosResourceTest {
    private static final File LOG_DIR = new File("testlogs/");

    private static final String CLIENT_1 = "andrew";
    private static final String CLIENT_2 = "bob";

    private PaxosResource paxosResource;

    @Before
    public void setUp() {
        paxosResource = PaxosResource.create(LOG_DIR);
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(LOG_DIR);
    }

    @Test
    public void canAddClients() {
        paxosResource.addClient(CLIENT_1);
        assertThat(paxosResource.getPaxosLearner(CLIENT_1)).isNotNull();
        assertThat(paxosResource.getPaxosAcceptor(CLIENT_1)).isNotNull();
    }

    @Test
    public void throwsIfTryingToAddClientTwice() {
        paxosResource.addClient(CLIENT_1);
        assertThatThrownBy(() -> paxosResource.addClient(CLIENT_1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void returnsNullIfClientNotAdded() {
        paxosResource.addClient(CLIENT_1);
        assertThat(paxosResource.getPaxosLearner(CLIENT_2)).isNull();
        assertThat(paxosResource.getPaxosAcceptor(CLIENT_2)).isNull();
    }
}
