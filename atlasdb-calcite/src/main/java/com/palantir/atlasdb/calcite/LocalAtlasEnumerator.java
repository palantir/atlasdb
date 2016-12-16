/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.calcite;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.linq4j.Enumerator;

public class LocalAtlasEnumerator implements Enumerator<AtlasRow> {
    private final Iterator<AtlasRow> iter;
    private final AtomicBoolean cancelFlag;
    private AtlasRow curResult;
    private boolean closed;

    public LocalAtlasEnumerator(Iterator<AtlasRow> iter, AtomicBoolean cancelFlag) {
        this.iter = iter;
        this.cancelFlag = cancelFlag;
        this.curResult = null;
        this.closed = false;
    }

    @Override
    public AtlasRow current() {
        if (isClosed() || curResult == null) {
            throw new NoSuchElementException();
        }
        return curResult;
    }

    @Override
    public boolean moveNext() {
        if (hasNext()) {
            curResult = iter.next();
            return true;
        }
        curResult = null;
        return false;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        closed = true;
    }

    public boolean hasNext() {
        if (iter == null || isClosed()) {
            return false;
        }
        return iter.hasNext();
    }

    public boolean isClosed() {
        if (cancelFlag.get()) {
            close();
        }
        return closed;
    }
}
