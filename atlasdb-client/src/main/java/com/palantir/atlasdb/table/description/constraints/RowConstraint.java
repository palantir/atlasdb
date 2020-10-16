/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.table.description.constraints;

/**
 * To implement this interface, you must implement two methods:
 *      "public static String getDescription()"
 *      "public static boolean validate(...)"
 *
 * The getDescription method should simply return a string describing what the constraint checks,
 * so that debugging becomes a lot easier. Feel free to put in new lines to make the message
 * more readable.
 *
 * For the validate method:
 *
 * For named tables, the parameters should match the list of rows/columns passed in the AtlasDb schema.
 * Row variables should come before column variables in the method signature.
 *
 * If I add a rowConstraints like: .addRowConstraint(RowConstraintMetadata.builder(SomeConstraint.class)
 *                                                  .addRowVariables("realm_id")
 *                                                  .addColumnVariables("data_event")),
 *
 * then the method signature should look like: public static boolean validate(long realmId, DataHistoryEvent dataEvent)
 *
 *
 * Dynamic tables work similarly, except column parameters and values are inputed as Lists. If you want the
 * value of a column to be passed in, use TableRenderer.DYNAMIC_COLUMN_VALUE_VARIABLE.
 * Row variables should come before column variables in the method signature.
 *
 * If I add a rowConstraint like:
 * .addRowConstraint(RowConstraintMetadata.builder(SomeConstraint.class)
 *         .addRowVariables("realm_id")
 *         .addColumnVariables("data_event_id", TableRenderer.DYNAMIC_COLUMN_VALUE_VARIABLE)),
 * then the method signature should look like:
 * public static boolean validate(long realmId,
 *                                List&lt;Long&gt; dataEventIds,
 *                                List&lt;LinkChangeSet&gt; linkChangeSets).
 *
 */
public interface RowConstraint extends Constraint {
    /*  Marker interface   */
}
