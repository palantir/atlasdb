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
package com.palantir.atlasdb.timelock.jsimpledb;

import java.util.NavigableSet;

import org.jsimpledb.JObject;
import org.jsimpledb.JTransaction;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.ObjId;

@JSimpleClass
public abstract class Timestamp implements JObject {
    public static Timestamp create(String client) {
        Timestamp timestamp = JTransaction.getCurrent().create(Timestamp.class);
        timestamp.setClient(client);
        return timestamp;
    }

    public abstract String getClient();
    public abstract void setClient(String client);

    public abstract long getTimestamp();
    public abstract void setTimestamp(long timestamp);

    public static NavigableSet<Timestamp> getAll() {
        return JTransaction.getCurrent().getAll(Timestamp.class);
    }

    public static Timestamp get(ObjId id) {
        return JTransaction.getCurrent().get(id, Timestamp.class);
    }
}
