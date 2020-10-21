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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableOkEvent.class)
@JsonDeserialize(as = ImmutableOkEvent.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(OkEvent.TYPE)
@Value.Immutable
public abstract class OkEvent implements Event {
    public static final String TYPE = "ok";

    public static final String UNLOCK_SUCCESS = "true";
    public static final String UNLOCK_FAILURE = "false";
    public static final String LOCK_SUCCESS = "Some Token";
    public static final String LOCK_FAILURE = "";
    public static final String REFRESH_SUCCESS = "Some Token";
    public static final String REFRESH_FAILURE = "";
    public static final String TIMESTAMP_SUCCESS = "1337";
    public static final String TIMESTAMP_FAILURE = "";

    @Override
    public abstract long time();

    @Override
    public abstract int process();

    @Nullable
    public abstract String value();

    @JsonProperty("f")
    public abstract String function();

    @Override
    public void accept(EventVisitor visitor) {
        visitor.visit(this);
    }
}
