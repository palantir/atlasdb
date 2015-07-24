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

package com.palantir.common.base;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;


public class ClosableIterators {
    private ClosableIterators() {/* */}

    public static <T> ClosableIterator<T> wrap(final Iterator<? extends T> it) {
        return new EmptyClose<T>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }
            @Override
            public T next() {
                return it.next();
            }
            @Override
            public void remove() {
                it.remove();
            }
        };
    }

    private abstract static class EmptyClose<T> implements ClosableIterator<T> {
        @Override
        public void close() {
            // do nothing
        }
    }

    public static <T> ClosableIterator<T> wrap(final Iterator<? extends T> it, final Closeable closable) {
        return new ClosableIterator<T>() {

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                return it.next();
            }

            @Override
            public void remove() {
                it.remove();
            }

            @Override
            public void close() {
                try {
                    closable.close();
                } catch (IOException e) {
                    Throwables.throwUncheckedException(e);
                }
            }
        };
    }
}
