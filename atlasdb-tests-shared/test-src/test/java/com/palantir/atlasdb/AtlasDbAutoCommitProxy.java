// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import com.google.common.base.Function;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

/**
 * Proxy which automatically wraps all calls to target interface in
 * transactions.
 */
public class AtlasDbAutoCommitProxy<T> implements InvocationHandler {
    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(Class<T> interfaceClass,
                                         TransactionManager txManager,
                                         Function<Transaction, T> delegateFunction) {
        return (T)Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] {interfaceClass},
                new AtlasDbAutoCommitProxy<T>(txManager, delegateFunction));
    }

    private final TransactionManager txManager;
    private final Function<Transaction, T> delegateFunction;

    private AtlasDbAutoCommitProxy(TransactionManager txManager,
                                      Function<Transaction, T> delegateFunction) {
        this.txManager = txManager;
        this.delegateFunction = delegateFunction;
    }

    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args) throws Throwable {
        return txManager.runTaskWithRetry(new TransactionTask<Object, Exception>() {
            @Override
            public Object execute(Transaction t) throws Exception {
                T delegate = delegateFunction.apply(t);
                return method.invoke(delegate, args);
            }
        });
    }
}
