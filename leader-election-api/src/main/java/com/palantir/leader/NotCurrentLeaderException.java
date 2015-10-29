/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.leader;

import com.google.common.net.HostAndPort;
import com.palantir.common.remoting.ServiceNotAvailableException;

/**
 * Some operations may only complete if they are run on the leader.  If a non-leader server gets one of these
 * requests then this exception may the thrown to indicate that this is the case.
 *
 * @author carrino
 */
public class NotCurrentLeaderException extends ServiceNotAvailableException {
    private static final long serialVersionUID = 1L;

    public NotCurrentLeaderException(String message, Throwable cause, HostAndPort leaderHint) {
        super(message, cause, leaderHint);
    }

    public NotCurrentLeaderException(String message, HostAndPort leaderHint) {
        super(message, leaderHint);
    }

    public NotCurrentLeaderException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotCurrentLeaderException(String message) {
        super(message);
    }
}
