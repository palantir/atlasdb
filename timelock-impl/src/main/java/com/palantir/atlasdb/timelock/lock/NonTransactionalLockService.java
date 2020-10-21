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
package com.palantir.atlasdb.timelock.lock;

import com.palantir.lock.LockService;
import com.palantir.lock.SimplifyingLockService;
import com.palantir.logsafe.UnsafeArg;
import javax.ws.rs.BadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This lock service may be used as a LockService, for the purposes of advisory locking as well as for
 * users to take locks outside of the transaction protocol.
 *
 * However, it does NOT allow transactions to take place, as it throws on attempts to acquire the immutable timestamp.
 * We rely on the previous implementation of SnapshotTransactionManager#getImmutableTimestampInternal (e.g. in 0.48.0),
 * which attempts to acquire the immutable timestamp before transactions begin running.
 */
public class NonTransactionalLockService extends SimplifyingLockService {
    private static final Logger log = LoggerFactory.getLogger(NonTransactionalLockService.class);

    private final LockService delegate;

    public NonTransactionalLockService(LockService delegate) {
        this.delegate = delegate;
    }

    @Override
    public LockService delegate() {
        return delegate;
    }

    @Override
    public Long getMinLockedInVersionId(String client) {
        log.warn(
                "Client {} attempted to getMinLockedInVersionId() on a non-transactional lock service!"
                        + " If you are using async timelock, this suggests that one of your AtlasDB clients still"
                        + " expects synchronous lock (i.e. is on a version of AtlasDB prior to 0.49.0). Please check"
                        + " that all AtlasDB clients are using AtlasDB >= 0.49.0.",
                UnsafeArg.of("client", client));
        throw new BadRequestException("getMinLockedInVersionId() not supported on non-transactional lock"
                + " service. Please consult the server logs for more detail.");
    }
}
