// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.common.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Future;

public class AsyncProxy<T> implements InvocationHandler {

    private final Future<T> futureResult;

    private AsyncProxy(Future<T> futureResult) {
        this.futureResult = futureResult;
    }

    @SuppressWarnings("unchecked")
    public static <T> T create(Future<T> futureResult, Class<? super T> iface) {
        AsyncProxy<T> proxy = new AsyncProxy<T>(futureResult);
        return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] { iface }, proxy);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return method.invoke(futureResult.get(), args);
    }
}
