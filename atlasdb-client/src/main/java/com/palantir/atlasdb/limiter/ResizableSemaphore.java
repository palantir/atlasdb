/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.limiter;

import java.util.concurrent.Semaphore;

/** An extension of Semaphore which allows dynamically shrinking the number of permits. */
public class ResizableSemaphore extends Semaphore {

    private volatile int currentPermits;

    public ResizableSemaphore(int permits) {
        super(permits);
        currentPermits = permits;
    }

    public void resize(int newPermits) {
        if (currentPermits != newPermits) {
            synchronized (this) {
                if (currentPermits != newPermits) {
                    adjustPermitsTo(newPermits);
                }
            }
        }
    }

    private void adjustPermitsTo(int newPermits) {
        if (newPermits > currentPermits) {
            release(newPermits - currentPermits);
        } else if (newPermits < currentPermits) {
            reducePermits(currentPermits - newPermits);
        }
        currentPermits = newPermits;
    }
}