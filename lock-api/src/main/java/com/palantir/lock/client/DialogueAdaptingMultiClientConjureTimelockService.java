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

import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockServiceBlocking;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Set;

public class DialogueAdaptingMultiClientConjureTimelockService {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");

    private final MultiClientConjureTimelockServiceBlocking dialogueDelegate;

    public DialogueAdaptingMultiClientConjureTimelockService(
            MultiClientConjureTimelockServiceBlocking dialogueDelegate) {
        this.dialogueDelegate = dialogueDelegate;
    }

    public LeaderTimes leaderTimes(Set<Namespace> namespaces) {
        return dialogueDelegate.leaderTimes(AUTH_HEADER, namespaces);
    }
}
