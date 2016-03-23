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
}
