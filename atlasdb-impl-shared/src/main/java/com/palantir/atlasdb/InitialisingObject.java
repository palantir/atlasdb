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

package com.palantir.atlasdb;

import com.google.common.collect.ForwardingObject;

public class InitialisingObject<T> extends ForwardingObject {
//    private T delegate;
//
//    private InitialisingObject(T sweeperService) {
//        delegate = sweeperService;
//    }
//
//    public static InitialisingObject createUninitialised() {
//        return new InitialisingObject<T>(null);
//    }
//
//    public static InitialisingSweeperService create(SweeperService sweeperService) {
//        return new InitialisingSweeperService(sweeperService);
//    }
//
//    public void initialise(SweeperService sweeperService) {
//        delegate = sweeperService;
//    }


    @Override
    protected Object delegate() {
        return null;
    }
}
