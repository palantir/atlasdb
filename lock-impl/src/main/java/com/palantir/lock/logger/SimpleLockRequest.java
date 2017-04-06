/**
 * Copyright 2017 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.lock.logger;

import com.palantir.lock.LockRequest;
import com.palantir.lock.TimeDuration;

public class SimpleLockRequest {
    private long lockCount;
    private long lockTimeout;
    private String lockGroupBehavior;
    private String blockingMode;
    private Long blockingDuration;
    private Long versionId;
    private String creatingThread;
    private String clientId;

    public SimpleLockRequest(LockRequest request) {
        setLockCount(request.getLocks().size());
        setLockTimeout(request.getLockTimeout().getTime());
        setLockGroupBehavior(request.getLockGroupBehavior().name());
        setBlockingMode(request.getBlockingMode().name());
        setBlockingDuration(request.getBlockingDuration());
        setVersionId(request.getVersionId());
        setCreatingThread(request.getCreatingThreadName());
    }

    public long getLockCount() {
        return lockCount;
    }

    public void setLockCount(long lockCount) {
        this.lockCount = lockCount;
    }

    public long getLockTimeout() {
        return lockTimeout;
    }

    public void setLockTimeout(long lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    public String getLockGroupBehavior() {
        return lockGroupBehavior;
    }

    public void setLockGroupBehavior(String lockGroupBehavior) {
        this.lockGroupBehavior = lockGroupBehavior;
    }

    public String getBlockingMode() {
        return blockingMode;
    }

    public void setBlockingMode(String blockingMode) {
        this.blockingMode = blockingMode;
    }

    public long getBlockingDuration() {
        return blockingDuration;
    }

    public void setBlockingDuration(TimeDuration blockingDuration) {
        this.blockingDuration = (blockingDuration != null) ? blockingDuration.getTime() : null;
    }

    public long getVersionId() {
        return versionId;
    }

    public void setVersionId(Long versionId) {
        this.versionId = versionId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getCreatingThread() {
        return creatingThread;
    }

    public void setCreatingThread(String creatingThread) {
        this.creatingThread = creatingThread;
    }
}