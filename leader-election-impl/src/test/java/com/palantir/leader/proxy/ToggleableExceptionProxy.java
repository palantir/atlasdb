package com.palantir.leader.proxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.Validate;

import com.palantir.common.base.Throwables;
import com.palantir.common.proxy.DelegatingInvocationHandler;

public class ToggleableExceptionProxy implements DelegatingInvocationHandler {

    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(Class<T> interfaceClass,
                                         T delegate,
                                         AtomicBoolean throwException,
                                         Exception exception) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                new ToggleableExceptionProxy(delegate, throwException, exception));
    }

    final Object delegate;
    final AtomicBoolean throwException;
    final Exception exception;

    private ToggleableExceptionProxy(Object delegate,
                                     AtomicBoolean throwException,
                                     Exception exception) {
        Validate.notNull(delegate);
        Validate.notNull(throwException);
        Validate.notNull(exception);
        this.delegate = delegate;
        this.throwException = throwException;
        this.exception = exception;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (throwException.get()) {
            throw Throwables.rewrap(exception);
        }
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Override
    public Object getDelegate() {
        return delegate;
    }

}
