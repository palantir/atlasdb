/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.table.description.constraints;

/**
 * To implement this interface, you must implement a method "public static List&lt;$SOMETUPLE$&gt; getKeys(...)"
 *
 * For named tables, the parameters should match the list of rows/columns passed in the AtlasDb schema.
 * Row variables should come before column variables in the method signature.
 *
 * If I add a foreign constraints like: .addForeignConstraint(ForeignConstraintMetadata
 *                                                  .builder("SomeTableName", SomeConstraint.class)
 *                                                  .addRowVariables("realm_id")
 *                                                  .addColumnVariables("data_event")),
 *
 * then the method signature should look like:
 *     public static List&lt;$SOMETUPLE$&gt; getKeys(long realmId, DataHistoryEvent dataEvent).
 *
 * The size of $SOMETUPLE$ should be equal to the number of fields in the key of "SomeTableName",
 * and the types should match accordingly.
 *
 * Dynamic tables work similarly, except column parameters and values are given as Lists. If you want the
 * value of a column to be passed in, use TableRenderer.DYNAMIC_COLUMN_VALUE_VARIABLE.
 * Row variables should come before column variables in the method signature.
 *
 * If I add a rowConstraint like: .addRowConstraint(RowConstraintMetadata.builder(SomeConstraint.class)
 *                                                  .addRowVariables("realm_id")
 *                                                  .addColumnVariables("data_event_id",
 *                                                        TableRenderer.DYNAMIC_COLUMN_VALUE_VARIABLE)),
 * then the method signature should look like: public static List&lt;$SOMETUPLE$&gt; getKeys(
 *                        long realmId, List&lt;Long&gt; dataEventIds, List&lt;LinkChangeSet&gt; linkChangeSets).
 * The size of $SOMETUPLE$ should be equal to the number of fields in the key of "SomeTableName",
 * and the types should match accordingly.
 */
public interface ForeignKeyConstraint {
    /* Marker interface */
}
