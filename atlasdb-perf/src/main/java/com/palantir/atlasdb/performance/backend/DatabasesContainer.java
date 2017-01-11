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
package com.palantir.atlasdb.performance.backend;

import java.util.List;

import com.google.common.collect.Lists;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;

public final class DatabasesContainer implements AutoCloseable {

    public static DatabasesContainer startup(List<KeyValueServiceInstrumentation> backends) {
        List<DockerizedDatabase> dbs = Lists.newArrayList();
        try {
            for (KeyValueServiceInstrumentation backend : backends) {
                DockerizedDatabase db = DockerizedDatabase.start(backend);
                Awaitility.await()
                        .atMost(Duration.FIVE_MINUTES)
                        .pollInterval(Duration.ONE_MINUTE)
                        .until(() -> backend.canConnect(db.getUri().getAddress()));
                dbs.add(db);
            }
            return new DatabasesContainer(dbs);
        } catch (Throwable t) {
            dbs.forEach(DockerizedDatabase::close);
            throw t;
        }
    }

    private final List<DockerizedDatabase> dbs;

    private DatabasesContainer(List<DockerizedDatabase> dbs) {
        this.dbs = dbs;
    }

    public List<DockerizedDatabase> getDockerizedDatabases() {
        return dbs;
    }

    @Override
    public void close() throws Exception {
        dbs.forEach(DockerizedDatabase::close);
    }
}
