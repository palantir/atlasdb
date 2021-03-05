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

package com.palantir.lock.client;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.ConjureLockV1Request;
import com.palantir.lock.ConjureLockV1ServiceBlocking;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockState;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.tokens.auth.AuthHeader;
import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;

public class DialogueComposingLockRpcClient implements LockRpcClient {
    private static final AuthHeader UNUSED_AUTH_HEADER = AuthHeader.valueOf("Bearer unused");

    private final ConjureLockV1ServiceBlocking pureDialogueDelegate;
    private final LockRpcClient dialogueShimDelegate;

    public DialogueComposingLockRpcClient(
            ConjureLockV1ServiceBlocking pureDialogueDelegate, LockRpcClient dialogueShimDelegate) {
        this.pureDialogueDelegate = pureDialogueDelegate;
        this.dialogueShimDelegate = dialogueShimDelegate;
    }

    @Override
    public Optional<LockResponse> lockWithFullLockResponse(String namespace, LockClient client, LockRequest request)
            throws InterruptedException {
        return dialogueShimDelegate.lockWithFullLockResponse(namespace, client, request);
    }

    @Override
    public boolean unlock(String namespace, HeldLocksToken token) {
        return dialogueShimDelegate.unlock(namespace, token);
    }

    @Override
    public boolean unlock(String namespace, LockRefreshToken token) {
        return dialogueShimDelegate.unlock(namespace, token);
    }

    @Override
    public boolean unlockSimple(String namespace, SimpleHeldLocksToken token) {
        return pureDialogueDelegate.unlockSimple(
                UNUSED_AUTH_HEADER, namespace, ConjureLockV1Tokens.getSimpleHeldLocksToken(token));
    }

    @Override
    public boolean unlockAndFreeze(String namespace, HeldLocksToken token) {
        return dialogueShimDelegate.unlockAndFreeze(namespace, token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(String namespace, LockClient client) {
        return dialogueShimDelegate.getTokens(namespace, client);
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(String namespace, Iterable<HeldLocksToken> tokens) {
        return dialogueShimDelegate.refreshTokens(namespace, tokens);
    }

    @Override
    public Optional<HeldLocksGrant> refreshGrant(String namespace, HeldLocksGrant grant) {
        return dialogueShimDelegate.refreshGrant(namespace, grant);
    }

    @Override
    public Optional<HeldLocksGrant> refreshGrant(String namespace, BigInteger grantId) {
        return dialogueShimDelegate.refreshGrant(namespace, grantId);
    }

    @Override
    public HeldLocksGrant convertToGrant(String namespace, HeldLocksToken token) {
        return dialogueShimDelegate.convertToGrant(namespace, token);
    }

    @Override
    public HeldLocksToken useGrant(String namespace, LockClient client, HeldLocksGrant grant) {
        return dialogueShimDelegate.useGrant(namespace, client, grant);
    }

    @Override
    public HeldLocksToken useGrant(String namespace, LockClient client, BigInteger grantId) {
        return dialogueShimDelegate.useGrant(namespace, client, grantId);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(String namespace) {
        return dialogueShimDelegate.getMinLockedInVersionId(namespace);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(String namespace, LockClient client) {
        return dialogueShimDelegate.getMinLockedInVersionId(namespace, client);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(String namespace, String client) {
        return dialogueShimDelegate.getMinLockedInVersionId(namespace, client);
    }

    @Override
    public LockServerOptions getLockServerOptions(String namespace) {
        return dialogueShimDelegate.getLockServerOptions(namespace);
    }

    @Override
    public Optional<LockRefreshToken> lock(String namespace, String client, LockRequest request)
            throws InterruptedException {
        return dialogueShimDelegate.lock(namespace, client, request);
    }

    @Override
    public Optional<HeldLocksToken> lockAndGetHeldLocks(String namespace, String client, LockRequest request) {
        return pureDialogueDelegate.lockAndGetHeldLocks(
                UNUSED_AUTH_HEADER,
                namespace,
                ConjureLockV1Request.builder()
                        .lockClient(client)
                        .lockRequest(request)
                        .build());
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(String namespace, Iterable<LockRefreshToken> tokens) {
        return ImmutableSet.copyOf(ConjureLockV1Tokens.getLegacyTokens(pureDialogueDelegate.refreshLockRefreshTokens(
                UNUSED_AUTH_HEADER, namespace, ConjureLockV1Tokens.getConjureTokens(tokens))));
    }

    @Override
    public long currentTimeMillis(String namespace) {
        return dialogueShimDelegate.currentTimeMillis(namespace);
    }

    @Override
    public void logCurrentState(String namespace) {
        dialogueShimDelegate.logCurrentState(namespace);
    }

    @Override
    public LockState getLockState(LockDescriptor lock) {
        return dialogueShimDelegate.getLockState(lock);
    }
}
