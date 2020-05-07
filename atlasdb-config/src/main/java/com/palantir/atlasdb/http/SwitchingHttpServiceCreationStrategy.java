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

package com.palantir.atlasdb.http;

import java.util.function.BooleanSupplier;
import java.util.function.Function;

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.common.proxy.PredicateSwitchedProxy;

public class SwitchingHttpServiceCreationStrategy implements HttpServiceCreationStrategy {
    private final BooleanSupplier shouldUseFirst;
    private final HttpServiceCreationStrategy first;
    private final HttpServiceCreationStrategy second;

    public SwitchingHttpServiceCreationStrategy(
            BooleanSupplier shouldUseFirst,
            HttpServiceCreationStrategy first,
            HttpServiceCreationStrategy second) {
        this.shouldUseFirst = shouldUseFirst;
        this.first = first;
        this.second = second;
    }

    @Override
    public <T> T createLiveReloadingProxyWithFailover(
            Class<T> type,
            AuxiliaryRemotingParameters clientParameters,
            String serviceName) {
        return createSwitchedProxy(
                strategy -> strategy.createLiveReloadingProxyWithFailover(type, clientParameters, serviceName), type);
    }

    private <T> T createSwitchedProxy(Function<HttpServiceCreationStrategy, T> factory, Class<T> type) {
        return PredicateSwitchedProxy.newProxyInstance(
                factory.apply(first),
                factory.apply(second),
                shouldUseFirst::getAsBoolean,
                type);
    }
}
