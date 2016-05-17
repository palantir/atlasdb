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

import com.palantir.exception.PalantirSqlException;

/**
 * An IdFactory is the core abstraction of ID generation.
 * @author eporter
 */
public interface IdFactory {

    /**
     * Gets a single ID.
     * @return The next ID from the backing sequence.
     * @throws PalantirSqlException
     */
    public long getNextId() throws PalantirSqlException;

    /**
     * Gets IDs in batch, which is far more efficient than getting IDs one at a time.
     * @param size
     * @return A new array with <code>size</code> new IDs.
     * @throws PalantirSqlException
     */
    public long [] getNextIds(int size) throws PalantirSqlException;

    /**
     * An alternative method to get new IDs without creating a new array every time.
     * @param ids An array to be completely filled with new IDs.
     * @throws PalantirSqlException
     */
    public void getNextIds(long [] ids) throws PalantirSqlException;
}
