/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.backup;

import com.palantir.atlasdb.backup.api.AtlasRestoreClient;
import com.palantir.atlasdb.backup.api.AtlasRestoreClientBlocking;
import com.palantir.atlasdb.backup.api.CompleteRestoreRequest;
import com.palantir.atlasdb.backup.api.CompleteRestoreResponse;
import com.palantir.tokens.auth.AuthHeader;

public class DialogueAdaptingAtlasRestoreClient implements AtlasRestoreClient {
    private final AtlasRestoreClientBlocking dialogueDelegate;

    public DialogueAdaptingAtlasRestoreClient(AtlasRestoreClientBlocking dialogueDelegate) {
        this.dialogueDelegate = dialogueDelegate;
    }

    @Override
    public CompleteRestoreResponse completeRestore(AuthHeader authHeader, CompleteRestoreRequest request) {
        return dialogueDelegate.completeRestore(authHeader, request);
    }
}
