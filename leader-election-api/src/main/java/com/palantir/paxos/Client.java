/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.paxos;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.BitSet;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableClient.class)
@JsonSerialize(as = ImmutableClient.class)
public abstract class Client {

    private static final BitSet allowedCharacters = new BitSet();
    private static final String VALIDATION_PATTERN_STRING = "[a-zA-Z0-9_-]+";

    static {
        allowedCharacters.set('A', 'Z' + 1);
        allowedCharacters.set('a', 'z' + 1);
        allowedCharacters.set('0', '9' + 1);
        allowedCharacters.set('-');
        allowedCharacters.set('_');
    }

    @JsonValue
    @Value.Parameter
    public abstract String value();

    @Value.Check
    protected final void check() {
        Preconditions.checkArgument(isValidClient(),
                "Error parsing client as it doesn't match pattern",
                SafeArg.of("pattern", VALIDATION_PATTERN_STRING),
                SafeArg.of("client", value()));
    }

    private boolean isValidClient() {
        int length = value().length();
        int cursor = 0;

        for (; cursor < length; cursor++) {
            if (!allowedCharacters.get(value().charAt(cursor))) {
                break;
            }
        }

        // Need at least one valid character
        return cursor != 0;

    }

    @JsonCreator
    public static Client of(String value) {
        return ImmutableClient.of(value);
    }

}
