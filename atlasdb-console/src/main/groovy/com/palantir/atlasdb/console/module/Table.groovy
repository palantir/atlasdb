package com.palantir.atlasdb.console.module

import com.palantir.atlasdb.api.TransactionToken
import com.palantir.atlasdb.console.AtlasConsoleServiceWrapper
import groovy.transform.CompileStatic

@CompileStatic
class Table {
    String name
    def desc = null
    AtlasConsoleServiceWrapper service

    Table(String name, AtlasConsoleServiceWrapper service) {
        this.name = name
        this.service = service
    }

    def describe() {
        AtlasCoreModule.pp(getDescription())
        return desc
    }

    def getDescription() {
        if(desc == null) {
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
        getDescription()['columns'].collect { it['long_name'] }
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
