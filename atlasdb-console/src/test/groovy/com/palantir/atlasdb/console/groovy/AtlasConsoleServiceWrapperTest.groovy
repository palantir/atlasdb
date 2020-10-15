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

import static groovy.test.GroovyAssert.assertEquals
import static groovy.test.GroovyAssert.shouldFail

import com.palantir.atlasdb.api.TransactionToken
import com.palantir.atlasdb.console.AtlasConsoleService
import com.palantir.atlasdb.console.AtlasConsoleServiceWrapper
import groovy.json.JsonSlurper
import org.gmock.WithGMock
import org.junit.Before
import org.junit.Test

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

    @Before
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
            assertEquals wrapper.tables(), ['a', 'b', 'c']
        }
    }

    @Test
    void testGetMetadata() {
        service.getMetadata(TABLE).returns(RESPONSE).once()
        slurper.parseText(RESPONSE).returns(RESULT).once()
        play {
            assertEquals wrapper.getMetadata(TABLE), RESULT
        }
    }

    @Test
    void testGetRows() {
        service.getRows(token, TO_JSON).returns(RESPONSE).once()
        slurper.parseText(RESPONSE).returns(RESULT).once()
        play {
            assertEquals wrapper.getRows(QUERY, token), RESULT
        }
    }

    @Test
    void testGetRowsAbortOnError() {
        service.getRows(token, TO_JSON).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.getRows(QUERY, token) }
            assertEquals EXCEPTION, thrownException
        }
    }

    @Test
    void testGetCells() {
        service.getCells(token, TO_JSON).returns(RESPONSE).once()
        slurper.parseText(RESPONSE).returns(RESULT).once()
        play {
            assertEquals wrapper.getCells(QUERY, token), RESULT
        }
    }

    @Test
    void testGetCellsAbortOnError() {
        service.getCells(token, TO_JSON).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.getCells(QUERY, token) }
            assertEquals EXCEPTION, thrownException
        }
    }

    @Test
    void testGetRange() {
        service.getRange(token, TO_JSON).returns(RESPONSE).once()
        slurper.parseText(RESPONSE).returns(RESULT).once()
        play {
            assertEquals wrapper.getRange(QUERY, token), RESULT
        }
    }

    @Test
    void testGetRangeAbortOnError() {
        service.getRange(token, TO_JSON).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.getRange(QUERY, token) }
            assertEquals EXCEPTION, thrownException
        }
    }

    @Test
    void testPut() {
        service.put(token, TO_JSON).returns(null).once()
        play {
            assertEquals wrapper.put(QUERY, token), null
        }
    }

    @Test
    void testPutAbortOnError() {
        service.put(token, TO_JSON).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.put(QUERY, token) }
            assertEquals EXCEPTION, thrownException
        }
    }

    @Test
    void testDelete() {
        service.delete(token, TO_JSON).returns(null).once()
        play {
            assertEquals wrapper.delete(QUERY, token), null
        }
    }

    @Test
    void testDeleteAbortOnError() {
        service.delete(token, TO_JSON).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.delete(QUERY, token) }
            assertEquals EXCEPTION, thrownException
        }
    }

    @Test
    void testCommit() {
        service.commit(token).returns(null).once()
        play {
            assertEquals wrapper.commit(token), null
        }
    }

    @Test
    void testAbort() {
        service.abort(token).returns(null).once()
        play {
            assertEquals wrapper.abort(token), null
        }
    }

    @Test
    void testStartTransaction() {
        service.startTransaction().returns(token).once()
        play {
            assertEquals wrapper.startTransaction(), token
        }
    }

    @Test
    void testEndTransactionCommit() {
        service.commit(token).returns(null).once()
        play {
            assertEquals wrapper.endTransaction(token), null
        }
    }

    @Test
    void testEndTransactionAbort() {
        service.commit(token).raises(EXCEPTION).once()
        service.abort(token).once()
        play {
            def thrownException = shouldFail(RuntimeException) { wrapper.endTransaction(token) }
            assertEquals EXCEPTION, thrownException
        }
    }
}
