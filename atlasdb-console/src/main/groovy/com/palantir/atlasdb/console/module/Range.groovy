/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.console.module

import com.palantir.atlasdb.api.TransactionToken
import com.palantir.atlasdb.console.AtlasConsoleServiceWrapper
import groovy.transform.CompileStatic

@CompileStatic
class Range implements Iterable {
    private Map start
    private AtlasConsoleServiceWrapper service
    private TransactionToken transactionToken

    public Range(AtlasConsoleServiceWrapper service, Map start, TransactionToken token) {
        this.service = service
        this.start = start
        this.transactionToken = token
    }

    public Iterator iterator() {
        def index = 0
        def node = start
        def data = { -> node['data']['data'] as List}
        def nextPage = { -> node['next'] }
        def hasNext = { ->
          index < data().size() || nextPage() != null
        }
        def next = { ->
          if(!hasNext())
              throw new NoSuchElementException()
          if (index >= data().size()) {
            node = service.getRange(nextPage(), transactionToken)
            index = 0
          }
          return data().get(index++)
        }
        return [ hasNext: hasNext, next: next] as Iterator
    }

    public first() {
        return iterator().hasNext() ? iterator().next() : null
    }

    public int count() {
        def total = 0
        this.each { total++ }
        return total
    }

    @Override
    public String toString() {
        "Range [start=" + start + ", token=" + transactionToken + "]"
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof Range)) {
            return false
        }
        Range other = (Range) obj
        return other.start == this.start && other.transactionToken == this.transactionToken
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 13 + start.hashCode();
        hash = hash * 31 + transactionToken.hashCode();
        return hash;
    }
}
