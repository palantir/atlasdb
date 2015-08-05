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
package com.palantir.common.proxy;

import com.palantir.common.supplier.RemoteContextHolder;

/**
 * A static method to return a proxy which simulates a remote server. The
 * returned proxy is a combination of {@link SerializingProxy},
 * {@link InterruptibleProxy}, and {@link DelayProxy}.
 *
 * @author jtamer
 */
public final class SimulatingServerProxy {

    public static <T> T newProxyInstance(Class<T> interfaceClass, T delegate, long sleep) {
        return RemoteContextHolder.newProxyCopyingOutboxToInbox(interfaceClass,
            SerializingProxy.newProxyInstance(interfaceClass,
                InterruptibleProxy.newProxyInstance(interfaceClass,
                    DelayProxy.newProxyInstance(interfaceClass, delegate, sleep),
                    CancelDelegate.CANCEL)));
    }

    private SimulatingServerProxy() {
        throw new AssertionError("uninstantiable");
    }
}
