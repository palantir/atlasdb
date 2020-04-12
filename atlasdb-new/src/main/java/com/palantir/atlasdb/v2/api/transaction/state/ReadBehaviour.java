/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.transaction.state;

public enum ReadBehaviour {
    IN_TRANSACTION, CHECKING_WRITE_WRITE_CONFLICTS, CHECKING_READ_WRITE_CONFLICTS;
//    interface Visitor<T> {
//        T inTransaction(long startTimestamp);
//        T checkingWriteWriteConflicts(long startTimestamp);
//        T checkingReadWriteConflicts(long startTimestamp, long commitTimestamp);
//    }
//
//    public abstract <T> T accept(Visitor<T> visitor);
//
//    private ReadBehaviour() {}
//
//    @Value.Immutable
//    static abstract class InTransaction extends ReadBehaviour {
//        abstract long startTimestamp();
//
//        @Override
//        public final <T> T accept(Visitor<T> visitor) {
//            return visitor.inTransaction(startTimestamp());
//        }
//    }
//
//    @Value.Immutable
//    static abstract class CheckingWriteWriteConflicts extends ReadBehaviour {
//        long startTimestamp();
//    }
//
//    @Value.Immutable
//    static abstract class CheckingReadWriteConflicts extends ReadBehaviour {
//        long startTimestamp();
//        long commitTimestamp();
//    }
}
