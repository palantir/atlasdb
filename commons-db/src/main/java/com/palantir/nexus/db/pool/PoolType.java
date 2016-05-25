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
package com.palantir.nexus.db.pool;

import javax.annotation.Nullable;

/**
 * Extensibility point for adding differing connection pool libraries.
 *
 * @author dstipp
 */

public enum PoolType {
    HIKARICP;

    /**
     * Looks up a PoolType by name. Names are not case-sensitive.
     *
     * @param strName the name of the type to lookup, typically corresponds to the type name, e.g.
     *        PoolType.HIKARICP.toString();
     * @return the property PoolType, or null if none exists
     * @throws IllegalArgumentException if requesting invalid pool.
     */
    @Nullable
    public static PoolType getTypeByName(@Nullable String strName) throws IllegalArgumentException {
        if (strName == null) {
            return null;
        }
        return PoolType.valueOf(strName.toUpperCase());
    }

}