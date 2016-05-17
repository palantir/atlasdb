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
 * An interface for a class which can produce IDs from a sequence.
 * @author eporter
 */
public interface IdGenerator {

    /**
     * Tries to generate up to <code>ids.length</code> ids from a database sequence.  Because some
     * IDs may not be valid, it can return less.
     * @param ids The array to be filled with IDs.
     * @return The count of how many valid IDs were put into the beginning of the array.
     */
    int generate(long [] ids) throws PalantirSqlException;
}
