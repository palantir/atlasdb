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
package com.palantir.lock.impl;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class ThreadCountLimiter {
    private int value;

    public ThreadCountLimiter(int value) {
        this.value = value;
    }

    synchronized public void set(int newValue) {
        value = newValue;
    }

    synchronized public boolean acquire() {
        if (value > 0) {
            value--;
            return true;
        }
        return false;
    }

    synchronized public void release() {
        value++;
    }
}
