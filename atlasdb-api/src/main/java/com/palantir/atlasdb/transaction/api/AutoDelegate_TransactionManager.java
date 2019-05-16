/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api;

import java.util.function.Supplier;

import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

public interface AutoDelegate_TransactionManager extends TransactionManager {
  TransactionManager delegate();

  @Override
  default TimelockService getTimelockService() {
    return delegate().getTimelockService();
  }

  @Override
  default TimelockServiceStatus getTimelockServiceStatus() {
    return delegate().getTimelockServiceStatus();
  }

  @Override
  default <T, E extends Exception> T finishRunTaskWithLockThrowOnConflict(
          TransactionAndImmutableTsLock tx, TransactionTask<T, E> task) throws E,
      TransactionFailedRetriableException {
    return delegate().finishRunTaskWithLockThrowOnConflict(tx, task);
  }

  @Override
  default <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionWithRetry(
          Supplier<C> conditionSupplier, ConditionAwareTransactionTask<T, C, E> task) throws E {
    return delegate().runTaskWithConditionWithRetry(conditionSupplier, task);
  }

  @Override
  default <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task) throws E,
      TransactionFailedRetriableException {
    return delegate().runTaskThrowOnConflict(task);
  }

  @Override
  default KeyValueServiceStatus getKeyValueServiceStatus() {
    return delegate().getKeyValueServiceStatus();
  }

  @Override
  default <T, E extends Exception> T runTaskWithLocksWithRetry(Iterable<HeldLocksToken> lockTokens,
          com.google.common.base.Supplier<LockRequest> guavaSupplier,
          LockAwareTransactionTask<T, E> task) throws E, InterruptedException,
      LockAcquisitionException {
    return runTaskWithLocksWithRetry(lockTokens, guavaSupplier, task);
  }

  @Override
  default <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionThrowOnConflict(
          C condition, ConditionAwareTransactionTask<T, C, E> task) throws E,
      TransactionFailedRetriableException {
    return delegate().runTaskWithConditionThrowOnConflict(condition, task);
  }

  @Override
  default <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionWithRetry(
          com.google.common.base.Supplier<C> guavaSupplier, ConditionAwareTransactionTask<T, C, E> task)
      throws E {
    return runTaskWithConditionWithRetry(guavaSupplier, task);
  }

  @Override
  default long getUnreadableTimestamp() {
    return delegate().getUnreadableTimestamp();
  }

  @Override
  default <T, E extends Exception> T runTaskWithLocksWithRetry(Supplier<LockRequest> lockSupplier,
          LockAwareTransactionTask<T, E> task) throws E, InterruptedException,
      LockAcquisitionException {
    return delegate().runTaskWithLocksWithRetry(lockSupplier, task);
  }

  @Override
  default void clearTimestampCache() {
    delegate().clearTimestampCache();
  }

  @Override
  default TransactionService getTransactionService() {
    return delegate().getTransactionService();
  }

  @Override
  default <T, E extends Exception> T runTaskWithLocksWithRetry(Iterable<HeldLocksToken> lockTokens,
          Supplier<LockRequest> lockSupplier, LockAwareTransactionTask<T, E> task) throws E,
      InterruptedException, LockAcquisitionException {
    return delegate().runTaskWithLocksWithRetry(lockTokens, lockSupplier, task);
  }

  @Override
  default TransactionAndImmutableTsLock setupRunTaskWithConditionThrowOnConflict(
          PreCommitCondition condition) {
    return delegate().setupRunTaskWithConditionThrowOnConflict(condition);
  }

  @Override
  default void registerClosingCallback(Runnable closingCallback) {
    delegate().registerClosingCallback(closingCallback);
  }

  @Override
  default <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E {
    return delegate().runTaskWithRetry(task);
  }

  @Override
  default <T, E extends Exception> T runTaskWithLocksThrowOnConflict(
          Iterable<HeldLocksToken> lockTokens, LockAwareTransactionTask<T, E> task) throws E,
      TransactionFailedRetriableException {
    return delegate().runTaskWithLocksThrowOnConflict(lockTokens, task);
  }

  @Override
  default Cleaner getCleaner() {
    return delegate().getCleaner();
  }

  @Override
  default long getImmutableTimestamp() {
    return delegate().getImmutableTimestamp();
  }

  @Override
  default TimestampManagementService getTimestampManagementService() {
    return delegate().getTimestampManagementService();
  }

  @Override
  default <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
    return delegate().runTaskReadOnly(task);
  }

  @Override
  default <T, E extends Exception> T runTaskWithLocksWithRetry(
          com.google.common.base.Supplier<LockRequest> guavaSupplier,
          LockAwareTransactionTask<T, E> task) throws E, InterruptedException,
      LockAcquisitionException {
    return runTaskWithLocksWithRetry(guavaSupplier, task);
  }

  @Override
  default void close() {
    delegate().close();
  }

  @Override
  default TimestampService getTimestampService() {
    return delegate().getTimestampService();
  }

  @Override
  default LockService getLockService() {
    return delegate().getLockService();
  }

  @Override
  default <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionReadOnly(
          C condition, ConditionAwareTransactionTask<T, C, E> task) throws E {
    return delegate().runTaskWithConditionReadOnly(condition, task);
  }

  @Override
  default KeyValueService getKeyValueService() {
    return delegate().getKeyValueService();
  }

  @Override
  default boolean isInitialized() {
    return delegate().isInitialized();
  }
}
