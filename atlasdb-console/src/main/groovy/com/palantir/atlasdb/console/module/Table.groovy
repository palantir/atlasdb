/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.console.module

import com.google.common.collect.Iterables
import com.palantir.atlasdb.api.TransactionToken
import com.palantir.atlasdb.console.AtlasConsoleJoins
import com.palantir.atlasdb.console.AtlasConsoleServiceWrapper
import groovy.transform.CompileStatic

@CompileStatic
class Table {
    String name
    def desc = null
    AtlasConsoleServiceWrapper service

    private static int DEFAULT_JOIN_BATCH_SIZE = 10000;

    Table(String name, AtlasConsoleServiceWrapper service) {
        this.name = name
        this.service = service
    }

    def describe() {
        AtlasCoreModule.pp(getDescription())
        return desc
    }

    def getDescription() {
        if (desc == null) {
            desc = service.getMetadata(name)
        }
        return desc
    }

    boolean isDynamic() {
        getDescription()['is_dynamic']
    }

    List columnNames() {
        if (isDynamic()) {
            println("columnNames cannot be called on a table with dynamic columns")
            return []
        }
        getDescription()['columns'].collect {it['long_name']}
    }

    List rowComponents() {
        getDescription()['row'].collect() {it['name']}
    }
    /**
     * Get data from atlas by specifying the row of data to return
     * @param row  A single row to get, where the row is a List of components
     * @param cols Optional List of columns to get, where each column is a
     *             String representing the column name. Defaults to all columns if unspecified.
     * @param token Optional TransactionToken representing current transaction.
     *              Defaults to TransactionToken.autoCommit() if unspecified.
     * @return Returns a Map object.
     *         For named columns:
     *         [
     *             "row": [{component1}, {component2}, ...],
     *             {column1Name}: {value},
     *             {column2Name}: {value},
     *             ...
     *         ]
     *
     *         For dynamic columns:
     *         [
     *             "row": [{component1}, {component2}, ...],
     *             "cols": [
     *             [
     *                 "col": [{component1}, ...],
     *                 "val": {value},
     *             ],
     *             ...
     *         ]
     */
    Map getRow(row, cols=null, TransactionToken token = service.getTransactionToken()) {
        def query = baseQuery()
        query['rows'] = [row]
        if (cols != null) {
            cols = listify(cols).collect { listify(it) }
            query['cols'] = cols
        }
        def result = service.getRows(query, token)['data'] as List
        return Iterables.getOnlyElement(result) as Map
    }

    /**
     * Get data from atlas by specifying the rows of data to return
     * @param rows List of rows to get, where each row is a List of components
     * @param cols Optional List of columns to get, where each column is a
     *             String representing the column name. Defaults to all columns if unspecified.
     * @param token Optional TransactionToken representing current transaction.
     *              Defaults to TransactionToken.autoCommit() if unspecified.
     * @return Returns a List of Map objects.
     *         For named colums:
     *         [
     *             "row": [{component1}, {component2}, ...],
     *             {column1Name}: {value},
     *             {column2Name}: {value},
     *             ...
     *         ]
     *
     *         For dynamic columns:
     *         [
     *             "row": [{component1}, {component2}, ...],
     *             "cols": [
     *             [
     *                 "col": [{component1}, ...],
     *                 "val": {value},
     *             ],
     *             ...
     *         ]
     */
    List getRows(rows, cols=null, TransactionToken token = service.getTransactionToken()) {
        def query = baseQuery()
        rows = listify(rows).collect { listify(it) }
        query['rows'] = rows
        if (cols != null) {
            cols = listify(cols).collect { listify(it) }
            query['cols'] = cols
        }
        return service.getRows(query, token)['data'] as List
    }

    /**
     * Like getRows, but requires columns to be specified.
     * @param rows List of rows to get, where each row is a List of components
     * @param cols List of columns to get, where each column is a String representing the column name.
     * @return Returns a List of Map objects in the same form as getRows
     */
    List getPartialRows(rows, cols, TransactionToken token = service.getTransactionToken()) {
        if (cols == null) {
            throw new IllegalArgumentException("cols must be a List of columns or a single column name")
        }
        getRows(rows, cols, token)
    }

    /**
     * Get data from atlas by specifying the rows and columns of data to return
     * @param cells List of Cell objects.
     *        For named columns:
     *        [
     *            "row": [{component1}, ...],
     *            "col": "columnName"
     *        ]
     *        For dynamic columns:
     *        [
     *            "row": [{component1}, ...],
     *            "col": [{component1}, ...]
     *        ]
     * @param token Optional TransactionToken representing current transaction.
     *              Defaults to TransactionToken.autoCommit() if unspecified.
     * @return List of Objects.
     *         For named columns:
     *         [
     *             "row": [{component1}, ...],
     *             {columnName}: {value}
     *         ]
     *         For dynamic columns:
     *         [
     *             "row": [{component1}, ...],
     *             "col": [{component1}, ...],
     *             "val": {value}
     *         ]
     */
    List getCells(cells, TransactionToken token = service.getTransactionToken()) {
        def query = baseQuery()
        def data = toListOfMaps(listify(cells), ['row', 'col'])
        query['data'] = data
        return service.getCells(query, token)['data'] as List
    }

    /**
     * Get an iterator over a range of table entries
     * @param rangeInfo A Map with the following optional keys:
     *        [
     *            "start": A row (or row prefix) to start the range at.
     *            "end": A row (or row prefix) to end the range at.
     *            "prefix": A row prefix to constrain the range to.
     *            "cols": A list of long column names to restrict the range to.
     *        ]
     * @return A Range iterable with a next() method that returns objects of the following form:
     *         For named columns:
     *         [
     *             "row": [{component1}, ...],
     *             {columnName}: {value}
     *         ]
     *         For dynamic columns:
     *         [
     *             "row": [{component1}, ...],
     *             "cols": [
     *             [
     *                 "col": [{component1}, ...],
     *                 "val": {value}
     *             ]
     *         ]
     */
    Range getRange(Map rangeInfo = null, TransactionToken token = service.getTransactionToken()) {
        rangeInfo = (rangeInfo == null ? [:] : rangeInfo)
        def query = [table:name as Object]
        rangeInfo.each { key, value ->
            query.put(key as String, listify(value))
        }
        return new Range(service, service.getRange(query, token) as Map, token)
    }

    /**
     * Lazily joins against an Iterable of key/value pairs.
     *
     *
     *
     * Tip: Since it is hard to lazily transform Iterables in Groovy, use Guava.
     *
     * Example:
     * <pre>
     * FluentIterable = com.google.common.collect.FluentIterable
     * input = FluentIterable.from(table("myTable").getRange()).transform{ [(it.row): it}
     * output = table("myOtherTable").join(input)
     * </pre>
     * @param input Iterable of Map<joinKey, inputValue> where JOIN_KEY is the row key of this table.
     * @param cols columns to select from this table.
     * @param batchSize size of getRows calls against this table, default 1000.
     * @param token.
     * @return Iterable of Maps with size 3, structured as
     * <pre>
     * { JOIN_KEY: joinKey, INPUT_VALUE: inputValue, OUTPUT_VALUE: outputValue}
     * </pre>
     * where joinKey and inputValue map to the inputs, and outputValue is the corresponding row in this table.
     */
    public Iterable<Map<String, Object>> join(
            Iterable<Map<?, ?>> input,
            cols = null,
            int batchSize = DEFAULT_JOIN_BATCH_SIZE,
            TransactionToken token = service.getTransactionToken()) {

        return AtlasConsoleJoins.join(input, batchSize, { keys -> getRows(keys, cols, token)});
    }


    void put(entries, TransactionToken token = service.getTransactionToken()) {
        def query = [table:name as Object]
        List data = []
        List entryList = listify(entries)
        if (isDynamic()) {
            data = entryList.collect { elem ->
                def map = [:]
                map.put('row', listify(elem['row']))
                map.put('col', listify(elem['col']))
                map.put('val', elem['val'])
                return map
            }
        }
        else {
            Map<String, Set> colFields = columnFields()
            data = entryList.collect { entry ->
                def result = [row: listify(entry['row'])]
                Map cols = entry['cols'] as Map
                Set colNames = this.columnNames().toSet()
                cols.each { key, value ->
                    if(!colNames.contains(key)) {
                        throw new IllegalArgumentException("Column ${key} does not exist")
                    }
                    Set<String> diff = []
                    diff.addAll((value as Map).keySet())
                    diff.removeAll(colFields[key] as Set<String>)
                    if (!diff.isEmpty()) {
                        throw new IllegalArgumentException("The following fields do not exist: " + diff)
                    }
                    result.put(key, value)
                }
                return result
            }
        }
        query['data'] = data
        service.put(query, token)
    }

    void delete(cells, TransactionToken token = service.getTransactionToken()) {
        def data = []
        listify(cells).each {
            def currentRowId = listify(it['row'])
            def currentColIds = listify(it['cols'])
            //service.delete expects one entry per row/col pair,
            //we allow multiple cols per row to be consistent with put so we need to convert here
            Map baseDataEntry = [row: currentRowId]
            currentColIds.each {
                def currentCol = isDynamic() ? listify(it) : it
                def baseDataEntryClone = baseDataEntry.clone()
                baseDataEntryClone['col'] = currentCol
                data.add(baseDataEntryClone)
            }
        }
        def query = baseQuery()
        query['data'] = data
        service.delete(query, token)
    }

    private List toListOfMaps(List list, List keys) {
        list.collect { elem ->
            def map = [:]
            for (String key in keys) {
                map.put(key, listify(elem.getAt(key)))
            }
            return map
        }
    }

    private baseQuery() {
        return ['table': name]
    }

    private List listify(obj) {
        (obj instanceof List ? obj : [obj]) as List
    }

    private Map<String, Set> columnFields() {
        return this.getDescription()['columns'].inject([:]) { map, col ->
            (map as Map)[col['long_name']] = col['value']['type']['fields'].collect { field ->
                return field['name']
            }.toSet()
            return map
        } as Map
    }
}
