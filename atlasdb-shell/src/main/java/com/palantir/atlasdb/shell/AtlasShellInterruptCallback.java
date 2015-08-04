/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.shell;

import java.util.Set;

import com.google.common.collect.Sets;

public class AtlasShellInterruptCallback {

    private final Set<AtlasShellInterruptListener> interruptListeners = Sets.newHashSet();

    public void interrupt() {
        synchronized(interruptListeners) {
            for (AtlasShellInterruptListener interruptListener : interruptListeners) {
                interruptListener.interrupt();
            }
        }
    }

    public void registerInterruptListener(AtlasShellInterruptListener interruptListener) {
        synchronized(interruptListeners) {
            interruptListeners.add(interruptListener);
        }
    }

    public void unregisterInterruptListener(AtlasShellInterruptListener interruptListener) {
        synchronized(interruptListeners) {
            interruptListeners.remove(interruptListener);
        }
    }
}
