/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.console.groovy

import org.codehaus.groovy.runtime.ScriptBytecodeAdapter
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

import com.palantir.atlasdb.api.TransactionToken
import com.palantir.atlasdb.console.AtlasConsoleService
import com.palantir.atlasdb.console.AtlasConsoleServiceWrapper
import groovy.json.JsonSlurper
import org.gmock.WithGMock

@WithGMock
class AtlasConsoleServiceWrapperTest {
    def service
    def slurper
    def token
    def wrapper
    def final TABLE = 't'
    def final QUERY = [a: 'b']
    def final TO_JSON = '{"a":"b"}'
    def final RESPONSE = '{"c":"d"}'
    def final RESULT = [c: 'd']
    def final EXCEPTION = new RuntimeException('error')

    @BeforeEach
    void setup() {
        service = mock(AtlasConsoleService)
        slurper = mock(JsonSlurper)
        token = mock(TransactionToken)
        wrapper = new AtlasConsoleServiceWrapper(service, slurper)
    }

    @Test
    void testTables() {
        def response = '["b", "a", "c"]'
        service.tables().returns(response).once()
        slurper.parseText(response).returns(['b', 'a', 'c']).once()
        play {
            assert wrapper.tables() == ['a', 'b', 'c']
        }
    }

    @Test
    void testGetMetadata() {
        service.getMetadata(TABLE).returns(RESPONSE).once()
        slurper.parseText(RESPONSE).returns(RESULT).once()
        play {
            assert wrapper.getMetadata(TABLE) == RESULT
        }
    }

    @Test
    void testGetRows() {
        service.getRows(token, TO_JSON).returns(RESPONSE).once()
        slurper.parseText(RESPONSE).returns(RESULT).once()
        play {
            assert wrapper.getRows(QUERY, token) == RESULT
        }
    }

    @Test
    void testGetRowsAbortOnError() {
        service.getRows(token, TO_JSON).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.getRows(QUERY, token) }
            assert EXCEPTION == thrownException
        }
    }

    @Test
    void testGetCells() {
        service.getCells(token, TO_JSON).returns(RESPONSE).once()
        slurper.parseText(RESPONSE).returns(RESULT).once()
        play {
            assert wrapper.getCells(QUERY, token) == RESULT
        }
    }

    @Test
    void testGetCellsAbortOnError() {
        service.getCells(token, TO_JSON).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.getCells(QUERY, token) }
            assert EXCEPTION == thrownException
        }
    }

    @Test
    void testGetRange() {
        service.getRange(token, TO_JSON).returns(RESPONSE).once()
        slurper.parseText(RESPONSE).returns(RESULT).once()
        play {
            assert wrapper.getRange(QUERY, token) == RESULT
        }
    }

    @Test
    void testGetRangeAbortOnError() {
        service.getRange(token, TO_JSON).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.getRange(QUERY, token) }
            assert EXCEPTION == thrownException
        }
    }

    @Test
    void testPut() {
        service.put(token, TO_JSON).returns(null).once()
        play {
            assert wrapper.put(QUERY, token) == null
        }
    }

    @Test
    void testPutAbortOnError() {
        service.put(token, TO_JSON).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.put(QUERY, token) }
            assert EXCEPTION == thrownException
        }
    }

    @Test
    void testDelete() {
        service.delete(token, TO_JSON).returns(null).once()
        play {
            assert wrapper.delete(QUERY, token) == null
        }
    }

    @Test
    void testDeleteAbortOnError() {
        service.delete(token, TO_JSON).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.delete(QUERY, token) }
            assert EXCEPTION == thrownException
        }
    }

    @Test
    void testCommit() {
        service.commit(token).returns(null).once()
        play {
            assert wrapper.commit(token) == null
        }
    }

    @Test
    void testAbort() {
        service.abort(token).returns(null).once()
        play {
            assert wrapper.abort(token) == null
        }
    }

    @Test
    void testStartTransaction() {
        service.startTransaction().returns(token).once()
        play {
            assert wrapper.startTransaction() == token
        }
    }

    @Test
    void testEndTransactionCommit() {
        service.commit(token).returns(null).once()
        play {
            assert wrapper.endTransaction(token) == null
        }
    }

    @Test
    void testEndTransactionAbort() {
        service.commit(token).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.endTransaction(token) }
            assert EXCEPTION == thrownException
        }
    }

    public static Throwable shouldFail(Class clazz, Closure code) {
        Throwable th = null;
        try {
            code.call();
        } catch (GroovyRuntimeException gre) {
            th = ScriptBytecodeAdapter.unwrap(gre);
        } catch (Throwable e) {
            th = e;
        }

        if (th == null) {
            fail("Closure " + code + " should have failed with an exception of type " + clazz.getName());
        } else if (!clazz.isInstance(th)) {
            fail("Closure " + code + " should have failed with an exception of type " + clazz.getName() + ", instead got Exception " + th);
        }
        return th;
    }
}
