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

package com.palantir.history.models;

import org.immutables.value.Value;

@Value.Immutable
public interface AcceptorUseCase {
    @Value.Parameter
    String value();

    static AcceptorUseCase createAcceptorUseCase(String paxosUseCase) {
        // TODO (someone): This is fragile because it MUST match timelock-impl's LocalPaxosComponents.
        // DO NOT CHANGE THIS VALUE WITHOUT A MIGRATION!
        return ImmutableAcceptorUseCase.of(String.format("%s!acceptor", paxosUseCase));
    }
}
