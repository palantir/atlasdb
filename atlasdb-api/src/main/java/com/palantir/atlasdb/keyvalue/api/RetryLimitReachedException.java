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

package com.palantir.atlasdb.keyvalue.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.palantir.common.annotations.ImmutablesStyles.PackageVisibleImmutablesStyle;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.SafeLoggable;
import java.util.Arrays;
import java.util.List;
import org.immutables.value.Value;

public class RetryLimitReachedException extends AtlasDbDependencyException implements SafeLoggable {

    private static final String MESSAGE = "Request was retried and failed each time for the request.";

    private final int numRetries;
    private final List<AttemptedTarget> attemptedTargets;

    public RetryLimitReachedException(List<Exception> exceptions) {
        super(MESSAGE);
        exceptions.forEach(this::addSuppressed);
        this.numRetries = exceptions.size();
        this.attemptedTargets = ImmutableList.of();
    }

    public RetryLimitReachedException(List<Exception> exceptions, List<AttemptedTarget> attemptedTargets) {
        super(MESSAGE);
        exceptions.forEach(this::addSuppressed);
        this.numRetries = exceptions.size();
        this.attemptedTargets = ImmutableList.copyOf(attemptedTargets);
    }

    public <E extends Exception> boolean suppressed(Class<E> type) {
        return Arrays.stream(getSuppressed()).anyMatch(type::isInstance);
    }

    @Override
    public String getLogMessage() {
        return MESSAGE;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return ImmutableList.of(SafeArg.of("numRetries", numRetries), SafeArg.of("attemptedTargets", attemptedTargets));
    }

    @Value.Immutable(builder = false)
    @PackageVisibleImmutablesStyle
    @JsonDeserialize(as = ImmutableAttemptedTarget.class)
    @JsonSerialize(as = ImmutableAttemptedTarget.class)
    public interface AttemptedTarget {

        @JsonProperty("name")
        @Value.Parameter
        String name();

        @JsonProperty("attempts")
        @Value.Parameter
        int attempts();

        static AttemptedTarget of(String name, int attempts) {
            return ImmutableAttemptedTarget.of(name, attempts);
        }
    }
}
