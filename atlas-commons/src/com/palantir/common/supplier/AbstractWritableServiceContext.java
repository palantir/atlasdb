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

package com.palantir.common.supplier;

import java.util.concurrent.Callable;

public abstract class AbstractWritableServiceContext<T> implements ServiceContext<T> {
    protected abstract void set(T newValue);

    @Override
    final public <R> R callWithContext(T context, Callable<R> callable) throws Exception {
        T oldValue = get();
        set(context);
        try {
            return callable.call();
        } finally {
            set(oldValue);
        }
    }

}
