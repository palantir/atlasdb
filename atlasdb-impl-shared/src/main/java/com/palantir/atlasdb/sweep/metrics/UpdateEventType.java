/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.metrics;

public enum UpdateEventType {
    ONE_ITERATION("currentIteration"),
    FULL_TABLE("currentTable"),
    ERROR("");

    private final String nameComponent;

    UpdateEventType(String nameComponent) {
        this.nameComponent = nameComponent;
    }

    public String getNameComponent() {
        return nameComponent;
    }
}
