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
package com.palantir.nexus.db.chunked;

import java.util.List;

import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;

/**
 * Visitor which visits a result set in chunks.
 *
 * @param <T> the type that each row is hydrated into.
 */
public interface ChunkedRowVisitor<T> {
    /**
     * Hydrates an individual row. Once a given number of individual rows
     * are hydrated, the hydrated values are aggregated into a chunk and
     * passed to {@link #processChunk(List)}.
     */
    public T hydrateRow(AgnosticLightResultRow row) throws PalantirSqlException;

    /**
     * Called when enough rows have been hydrated to form a chunk, or when
     * the result set is exhausted.
     *
     * @return true if further chunks should be visited, or false if
     *         visiting should be aborted.
     */
    public boolean processChunk(List<T> chunk) throws PalantirSqlException;
}
