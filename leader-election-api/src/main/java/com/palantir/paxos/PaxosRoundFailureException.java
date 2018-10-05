/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.paxos;

/**
 * Thrown when a paxos proposal round fails due to inability to establish quorum support.
 *
 * @author rullman
 */
public class PaxosRoundFailureException extends Exception {
    private static final long serialVersionUID = 1L;

    public PaxosRoundFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public PaxosRoundFailureException(String message) {
        super(message);
    }
}
