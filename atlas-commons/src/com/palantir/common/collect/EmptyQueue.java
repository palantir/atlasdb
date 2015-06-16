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

package com.palantir.common.collect;

import java.util.AbstractQueue;
import java.util.Iterator;

import com.google.common.collect.Iterators;

/**
 * This queue is empty and will neither return nor accept any elements
 * @author carrino
 *
 * @param <E>
 */
public class EmptyQueue<E> extends AbstractQueue<E> {
    private static final EmptyQueue<?> INSTANCE = new EmptyQueue<Object>();

    @SuppressWarnings("unchecked")
    public static <E> EmptyQueue<E> of() {
        return (EmptyQueue<E>)INSTANCE;
    }

    private EmptyQueue() {
        /* */
    }

    @Override
    public boolean offer(E o) {
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        return Iterators.emptyIterator();
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public E peek() {
        return null;
    }

    @Override
    public E poll() {
        return null;
    }
}
