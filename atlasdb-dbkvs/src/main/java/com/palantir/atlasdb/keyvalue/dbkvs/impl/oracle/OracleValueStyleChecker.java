/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyle;
import com.palantir.exception.PalantirSqlException;

class OracleValueStyleChecker {
    private final OracleDdlConfig ddlConfig;
    private final ConnectionSupplier conns;

    OracleValueStyleChecker(OracleDdlConfig ddlConfig, ConnectionSupplier conns) {
        this.ddlConfig = ddlConfig;
        this.conns = conns;
    }

    /**
     * Checks that according to the database our {@link ConnectionSupplier} connects us to, the metadata table is not
     * in disagreement with whether we think we need an overflow table or not.
     * Does not throw if we are unable to read a value from the metadata table. Although this method is only expected
     * to be called if the table actually exists, it is possible for multiple nodes in an HA configuration to race
     * as creating a table and writing its metadata are not atomic operations.
     *
     * @param tableRef table reference to check the {@link TableValueStyle} for
     * @param needsOverflow whether we expect that the type is OVERFLOW or RAW
     * @throws IllegalStateException if the metadata table and our expected value of needsOverflow are in disagreement.
     */
    void checkValueStyle(TableReference tableRef, boolean needsOverflow) {
        String checkValueStyleQuery =
                "SELECT table_size FROM "
                        + ddlConfig.metadataTable().getQualifiedName()
                        + " WHERE table_name = " + tableRef.getQualifiedName();
        try {
            int valueStyle = Ints.checkedCast(conns.get().selectLongUnregisteredQuery(checkValueStyleQuery));
            int expectedValueStyle = needsOverflow ? TableValueStyle.OVERFLOW.getId() : TableValueStyle.RAW.getId();
            Preconditions.checkState(valueStyle == expectedValueStyle,
                    "Found a value style with ID %s stored in the database, but we need it to be %s."
                            + " This may have occurred because a table's schema was extended and it now needs an"
                            + " overflow table while it previously did not. Please contact support for assistance.",
                    TableValueStyle.byId(valueStyle),
                    TableValueStyle.byId(expectedValueStyle));
        } catch (PalantirSqlException e) {
            // Thrown if the table is not found in the Oracle DDL metadata table.
        }
    }
}
