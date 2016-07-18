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
 *
 */

package com.palantir.atlasdb.performance.backend;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * The physical store backing the AtlasDB key-value service. Any new physical store should extend this class (currently only Postgres is
 * implemented).
 */
public abstract class PhysicalStore implements AutoCloseable {

    public abstract KeyValueService connect();

    public static PhysicalStore create(KeyValueServiceType type) {
        switch (type) {
            case POSTGRES:
                return new PostgresPhysicalStore();
            case ORACLE:
            case CASSANDRA:
                throw new NotImplementedException();
        }
        throw new NotImplementedException();
    }

}
