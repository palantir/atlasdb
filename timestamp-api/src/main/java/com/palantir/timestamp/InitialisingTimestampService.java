/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timestamp;

import com.google.common.collect.ForwardingObject;

public class InitialisingTimestampService extends ForwardingObject implements TimestampService {
    private TimestampService delegate;

    private InitialisingTimestampService(TimestampService timestampService) {
        delegate = timestampService;
    }

    public static InitialisingTimestampService createUninitialised() {
        return new InitialisingTimestampService(null);
    }

    public static InitialisingTimestampService create(TimestampService timestampService) {
        return new InitialisingTimestampService(timestampService);
    }

    public void initialise(TimestampService timestampService) {
        delegate = timestampService;
    }

    private TimestampService getDelegate() {
        return (TimestampService) delegate();
    }

    @Override
    protected Object delegate() {
        checkInitialised();
        return delegate;    }

    @Override
    public long getFreshTimestamp() {
        return getDelegate().getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return getDelegate().getFreshTimestamps(numTimestampsRequested);
    }

    void checkInitialised() {
        if (delegate == null) {
            throw new IllegalStateException("Not initialised");
        }
    }
}
