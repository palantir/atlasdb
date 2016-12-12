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

import java.util.NoSuchElementException;

import org.apache.calcite.linq4j.Enumerator;

public class AtlasEnumerator implements Enumerator<Object[]> {
    private boolean ready = false;

    @Override
    public Object[] current() {
        if (ready) {
            return new String[] { "entry1" };
        }
        throw new NoSuchElementException();
    }

    @Override
    public boolean moveNext() {
        boolean ret = ready;
        ready = true;
        return !ret;
    }

    @Override
    public void reset() {
        ready = false;
    }

    @Override
    public void close() {
        // nothing to close
    }
}
