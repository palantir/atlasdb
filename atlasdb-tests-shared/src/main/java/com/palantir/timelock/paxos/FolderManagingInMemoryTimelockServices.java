/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.paxos;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.timestamp.TimestampService;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class FolderManagingInMemoryTimelockServices implements TestRule {
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final AtomicReference<InMemoryTimelockServices> inMemoryServicesReference = new AtomicReference<>();

    @Override
    public Statement apply(
            Statement base, Description description) {
        return temporaryFolder.apply(new Statement() {
            @Override
            public void evaluate() throws Throwable {
                InMemoryTimelockServices services = InMemoryTimelockServices.create(temporaryFolder);
                inMemoryServicesReference.set(services);
            }
        }, description);
    }

    public TimestampService getTimestampService() {
        return Optional.ofNullable(inMemoryServicesReference.get())
                .map(InMemoryTimelockServices::getTimestampService)
                .orElseThrow(() -> new SafeIllegalStateException("Retrieved timestamp service before initialization"));
    }
}
