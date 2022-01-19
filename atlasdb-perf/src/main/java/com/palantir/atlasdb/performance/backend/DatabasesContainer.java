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
package com.palantir.atlasdb.performance.backend;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.awaitility.Awaitility;

public final class DatabasesContainer implements AutoCloseable {

    public static DatabasesContainer startup(List<KeyValueServiceInstrumentation> backends) {
        List<DockerizedDatabase> dbs = new ArrayList<>();
        try {
            for (KeyValueServiceInstrumentation backend : backends) {
                DockerizedDatabase db = DockerizedDatabase.start(backend);
                Awaitility.await()
                        .atMost(Duration.ofMinutes(5))
                        .pollInterval(Duration.ofSeconds(5))
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
    public void close() {
        dbs.forEach(DockerizedDatabase::close);
    }
}
