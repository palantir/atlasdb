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

package com.palantir.atlasdb.refactorings;

import com.google.errorprone.refaster.Refaster;
import com.google.errorprone.refaster.annotation.AfterTemplate;
import com.google.errorprone.refaster.annotation.BeforeTemplate;
import com.palantir.atlasdb.factory.ImmutableTransactionManagers.GlobalMetricsRegistryBuildStage;
import com.palantir.atlasdb.factory.ImmutableTransactionManagers.UserAgentBuildStage;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgents;

public final class UseTypedUserAgentVsString {

    @BeforeTemplate
    GlobalMetricsRegistryBuildStage userAgentStringTransforms(
            UserAgentBuildStage userAgentBuildStage,
            UserAgent userAgent) {
        return userAgentBuildStage.userAgent(Refaster.anyOf(
                userAgent.toString(),
                UserAgents.format(userAgent)));
    }

    @AfterTemplate
    GlobalMetricsRegistryBuildStage useTypedUserAgent(UserAgentBuildStage userAgentBuildStage, UserAgent userAgent) {
        return userAgentBuildStage
                .userAgent(userAgent);
    }

}
