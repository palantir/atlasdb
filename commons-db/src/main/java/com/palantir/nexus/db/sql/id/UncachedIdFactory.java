/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.nexus.db.sql.id;

import org.apache.commons.lang.Validate;

import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.ReentrantConnectionSupplier;

/**
 * An IdFactory which does not cache any IDs and goes to the source IdGenerator every time.
 * @author eporter
 *
 */
public class UncachedIdFactory implements IdFactory {

    // Use an IdGenerator which is guaranteed to fill up the entire array.
    private final DbSequenceBackedIdGenerator idGenerator;
    private final String sequenceName;

    public UncachedIdFactory(DBType dbType, String sequenceName, ReentrantConnectionSupplier connectionSupplier) {
        idGenerator = new DbSequenceBackedIdGenerator(dbType, sequenceName, connectionSupplier);
        this.sequenceName = sequenceName;
    }

    @Override
    public long getNextId() throws PalantirSqlException {
        return getNextIds(1)[0];
    }

    @Override
    public long [] getNextIds(int size) throws PalantirSqlException {
        long [] ids = new long[size];
        int generated = idGenerator.generate(ids);
        Validate.isTrue(generated == size, "The backing ID generator is broken."); //$NON-NLS-1$
        return ids;
    }

    @Override
    public void getNextIds(long[] ids) throws PalantirSqlException {
        idGenerator.generate(ids);
    }

    public String getBackingSequence() {
        return sequenceName;
    }

}
