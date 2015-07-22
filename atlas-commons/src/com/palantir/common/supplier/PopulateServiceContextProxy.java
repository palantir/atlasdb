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

package com.palantir.common.supplier;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Callable;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.common.base.Throwables;
import com.palantir.common.proxy.AbstractDelegatingInvocationHandler;

/**
 * This proxy will inject the value from the {@link Supplier} (or constant value) into the
 * {@link ServiceContext} right before each call to the returned proxy.
 * <p>
 * If using the reentrant version and the {@link ServiceContext} already has a non-null value
 * then a new value will not be set from the {@link Supplier}
 *
 * @author carrino
 */
public class PopulateServiceContextProxy<T, S> extends AbstractDelegatingInvocationHandler {
    @SuppressWarnings("unchecked")
    public static <T, S> T newProxyInstance(Class<T> interfaceClass,
                                            T delegate,
                                            Supplier<? extends S> supplier,
                                            ServiceContext<S> contextToSetFromSupplier) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                new PopulateServiceContextProxy<T, S>(delegate, supplier, contextToSetFromSupplier, false));
    }

    @SuppressWarnings("unchecked")
    public static <T, S> T newProxyInstanceWithConstantValue(Class<T> interfaceClass,
                                                        T delegate,
                                                        S staticValue,
                                                        ServiceContext<S> contextToSetFromSupplier) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                new PopulateServiceContextProxy<T, S>(delegate, Suppliers.ofInstance(staticValue), contextToSetFromSupplier, false));
    }

    @SuppressWarnings("unchecked")
    public static <T, S> T newReentrantProxyInstance(Class<T> interfaceClass,
                                                     T delegate,
                                                     Supplier<? extends S> supplier,
                                                     ServiceContext<S> contextToSetFromSupplier) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                new PopulateServiceContextProxy<T, S>(delegate, supplier, contextToSetFromSupplier, true));
    }

    final private T delegate;
    final private Supplier<? extends S> supplier;
    final private ServiceContext<S> context;
    final private boolean reentrant;

    private PopulateServiceContextProxy(T delegate,
                                        Supplier<? extends S> supplier,
                                        ServiceContext<S> context,
                                        boolean reentrant) {
        this.delegate = Preconditions.checkNotNull(delegate);
        this.supplier = Preconditions.checkNotNull(supplier);
        this.context = Preconditions.checkNotNull(context);
        this.reentrant = reentrant;
    }

    @Override
    public T getDelegate() {
        return delegate;
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        if (reentrant && context.get() != null) {
            return super.invoke(proxy, method, args);
        }

        Callable<Object> callable = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    return PopulateServiceContextProxy.super.invoke(proxy, method, args);
                } catch (Throwable e) {
                    Throwables.throwIfInstance(e, Exception.class);
                    throw Throwables.throwUncheckedException(e);
                }
            }
        };

        return context.callWithContext(supplier.get(), callable);
    }

}
