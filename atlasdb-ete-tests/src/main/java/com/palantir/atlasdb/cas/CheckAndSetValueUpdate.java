/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.cas;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CheckAndSetValueUpdate {
    private final Optional<Long> oldValue;
    private final Optional<Long> newValue;

    @JsonCreator
    public CheckAndSetValueUpdate(
            @JsonProperty(value = "oldValue", required = true) Optional<Long> oldValue,
            @JsonProperty(value = "newValue", required = true) Optional<Long> newValue) {
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    public Optional<Long> getOldValue() {
        return oldValue;
    }

    public Optional<Long> getNewValue() {
        return newValue;
    }
}
