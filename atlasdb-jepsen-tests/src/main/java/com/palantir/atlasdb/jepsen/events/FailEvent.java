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
package com.palantir.atlasdb.jepsen.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableFailEvent.class)
@JsonDeserialize(as = ImmutableFailEvent.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(FailEvent.TYPE)
@Value.Immutable
public abstract class FailEvent implements Event {
    public static final String TYPE = "fail";

    @Override
    public abstract long time();

    @Override
    public abstract int process();

    public abstract String error();

    @Override
    public void accept(EventVisitor visitor) {
        visitor.visit(this);
    }
}
