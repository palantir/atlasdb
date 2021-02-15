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
package com.palantir.atlasdb.console

import com.palantir.atlasdb.api.TransactionToken
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic

@CompileStatic
class AtlasConsoleServiceWrapper {
    private final AtlasConsoleService service
    private final JsonSlurper slurper
    private TransactionToken token
    private List transactionLog

    protected AtlasConsoleServiceWrapper(AtlasConsoleService service, JsonSlurper slurper) {
        this.service = service
        this.slurper = slurper
        this.token = TransactionToken.autoCommit()
        this.transactionLog = []
    }

    static AtlasConsoleServiceWrapper init(AtlasConsoleService service) {
        return new AtlasConsoleServiceWrapper(service, new JsonSlurper())
    }

    List tables() throws IOException {
        def result = slurp(service.tables()) as List
        return result.sort()
    }

    Map getMetadata(String table) throws IOException {
        slurp(service.getMetadata(table)) as Map
    }

    def truncate(String table) throws IOException {
        addToTransactionLog("truncate: " + table)
        service.truncate(table)
    }

    def getRows(query, TransactionToken token = this.token) throws IOException {
        addToTransactionLog("getRows: " + query.toString())
        abortIfError({ -> slurp(service.getRows(token, spit(query))) }, token)
    }

    def getCells(query, TransactionToken token = this.token) throws IOException {
        addToTransactionLog("getCells: " + query.toString())
        abortIfError({ slurp(service.getCells(token, spit(query))) }, token)
    }

    Map getRange(query, TransactionToken token = this.token) throws IOException {
        addToTransactionLog("getRange: " + query.toString())
        abortIfError({ slurp(service.getRange(token, spit(query))) }, token) as Map
    }

    void put(data, TransactionToken token = this.token) throws IOException {
        addToTransactionLog("put: " + data.toString())
        abortIfError({ service.put(token, spit(data)) }, token)
    }

    void delete(data, TransactionToken token = this.token) throws IOException {
        addToTransactionLog("delete: " + data.toString())
        abortIfError({ service.delete(token, spit(data)) }, token)
    }

    TransactionToken startTransaction() {
        this.token = service.startTransaction()
        this.transactionLog = []
        addToTransactionLog(this.token.toString())
        addToTransactionLog("Transaction started")
        return this.token
    }

    void endTransaction(TransactionToken token = this.token) {
        abortIfError({ -> commit(token) }, token)
        this.token = TransactionToken.autoCommit()
        this.transactionLog = []
    }

    void commit(TransactionToken token = this.token) {
        service.commit(token)
        this.token = TransactionToken.autoCommit()
        this.transactionLog = []
    }

    void abort(TransactionToken token = this.token) {
        service.abort(token)
        this.token = TransactionToken.autoCommit()
        this.transactionLog = []
    }

    TransactionToken getTransactionToken() {
        return this.token
    }

    List currentTransaction() {
        return this.transactionLog
    }

    private addToTransactionLog(String log) {
        if (this.token) {
            this.transactionLog.add(log)
        }
    }

    private abortIfError(Closure operation, TransactionToken token) {
        try {
            return operation()
        }
        catch(e) {
            abort(token)
            throw e
        }
    }

    private slurp(String input) {
        slurper.parseText(input)
    }

    private String spit(obj) {
       JsonOutput.toJson(obj)
    }
}
