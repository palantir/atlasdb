/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.v2;

import java.time.Duration;
import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonSerialize(as = ImmutableLeasableStartIdentifiedAtlasDbTransactionResponse.class)
@JsonDeserialize(as = ImmutableLeasableStartIdentifiedAtlasDbTransactionResponse.class)
public interface LeasableStartIdentifiedAtlasDbTransactionResponse {
    @Value.Parameter
    StartIdentifiedAtlasDbTransactionResponse getStartTransactionResponse();

    @Value.Parameter
    Optional<Duration> getLeasePeriod();

    static LeasableStartIdentifiedAtlasDbTransactionResponse of(StartIdentifiedAtlasDbTransactionResponse response) {
        return ImmutableLeasableStartIdentifiedAtlasDbTransactionResponse.of(
                response,
                Optional.empty());
    }

    static LeasableStartIdentifiedAtlasDbTransactionResponse of(
            StartIdentifiedAtlasDbTransactionResponse response,
            Duration leasePeriod) {
        return ImmutableLeasableStartIdentifiedAtlasDbTransactionResponse.of(
                response,
                Optional.of(leasePeriod));
    }
}
