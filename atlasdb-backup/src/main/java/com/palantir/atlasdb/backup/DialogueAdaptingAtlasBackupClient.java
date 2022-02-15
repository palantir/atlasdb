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

import com.palantir.atlasdb.backup.api.AtlasBackupClient;
import com.palantir.atlasdb.backup.api.AtlasBackupClientBlocking;
import com.palantir.atlasdb.backup.api.CompleteBackupRequest;
import com.palantir.atlasdb.backup.api.CompleteBackupResponse;
import com.palantir.atlasdb.backup.api.PrepareBackupRequest;
import com.palantir.atlasdb.backup.api.PrepareBackupResponse;
import com.palantir.tokens.auth.AuthHeader;

public class DialogueAdaptingAtlasBackupClient implements AtlasBackupClient {
    private final AtlasBackupClientBlocking dialogueDelegate;

    public DialogueAdaptingAtlasBackupClient(AtlasBackupClientBlocking dialogueDelegate) {
        this.dialogueDelegate = dialogueDelegate;
    }

    @Override
    public PrepareBackupResponse prepareBackup(AuthHeader authHeader, PrepareBackupRequest request) {
        return dialogueDelegate.prepareBackup(authHeader, request);
    }

    @Override
    public CompleteBackupResponse completeBackup(AuthHeader authHeader, CompleteBackupRequest request) {
        return dialogueDelegate.completeBackup(authHeader, request);
    }
}
