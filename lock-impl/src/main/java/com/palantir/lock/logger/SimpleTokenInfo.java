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
package com.palantir.lock.logger;

import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockMode;
import com.palantir.logsafe.Preconditions;
import java.util.Date;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public abstract class SimpleTokenInfo {
    public static SimpleTokenInfo of(HeldLocksToken token, LockMode lockMode) {
        return ImmutableSimpleTokenInfo.builder()
                .lockMode(lockMode)
                .expiresIn(token.getExpirationDateMs() - System.currentTimeMillis())
                .createdAtTs(token.getCreationDateMs())
                .tokenId(token.getTokenId().toString())
                .clientId(Preconditions.checkNotNull(token.getClient()).getClientId())
                .requestThread(token.getRequestingThread())
                .createAt(new Date(token.getCreationDateMs()).toString())
                .versionId(token.getVersionId())
                .build();
    }

    @Value.Parameter
    public abstract LockMode getLockMode();

    @Value.Parameter
    public abstract long getExpiresIn();

    @Value.Parameter
    public abstract long getCreatedAtTs();

    @Value.Parameter
    public abstract String getTokenId();

    @Value.Parameter
    public abstract String getClientId();

    @Value.Parameter
    public abstract String getRequestThread();

    @Value.Parameter
    public abstract String getCreateAt();

    @Nullable
    @Value.Parameter
    public abstract Long getVersionId();
}
