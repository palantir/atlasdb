/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.management;

import com.palantir.atlasdb.keyvalue.api.TimestampSeriesProvider;
import java.nio.file.Path;
import javax.sql.DataSource;
import org.derive4j.Data;

@Data
public interface PersistentNamespaceContext {
    interface Cases<R> {
        R timestampBoundPaxos(Path fileDataDirectory, DataSource sqliteDataSource);

        R dbBound(TimestampSeriesProvider seriesProvider);
    }

    <R> R match(PersistentNamespaceContext.Cases<R> cases);
}
