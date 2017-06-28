package com.palantir.atlasdb.console.groovy

import com.palantir.atlasdb.console.AtlasConsoleServiceWrapper
import com.palantir.atlasdb.console.module.Range
import com.palantir.atlasdb.console.module.Table
import org.junit.Assert

import static groovy.test.GroovyAssert.assertEquals
import static groovy.test.GroovyAssert.shouldFail

import org.gmock.WithGMock
import org.junit.Before
import org.junit.Test

import com.palantir.atlasdb.api.TransactionToken

@WithGMock
class TableTest {
    AtlasConsoleServiceWrapper service
    Table table

    final String TABLE_NAME = 't'

    final tableQuery1 = 'a'
    final tableQuery2=['a', 'b']
    final tableQuery3=[['a', 'b']]
    final tableQuery4=[['a', 'b'], ['c', 'd']]

    final serviceQuery1=[[tableQuery1]]
    final serviceQuery2=[[tableQuery2[0]], [tableQuery2[1]]]
    final serviceQuery3=tableQuery3
    final serviceQuery4=tableQuery4

    final tableQueryToServiceQuery = [
        (tableQuery1): serviceQuery1,
        (tableQuery2): serviceQuery2,
        (tableQuery3): serviceQuery3,
        (tableQuery4): serviceQuery4
    ]

    @Before
    void setup() {
        service = mock(AtlasConsoleServiceWrapper)
        table = new Table(TABLE_NAME, service)
    }

    @Test
    void testDescribe() {
        final Map DESC = [
            isDynamic:false,
            row: [
                [name: 'foo'],
                [name: 'bar']
            ],
            columns: [
                [long_name: 'alice'],
                [long_name: 'bob']
            ]
        ]
        service.getMetadata(TABLE_NAME).returns(DESC).once()
        play {
            assertEquals(DESC, table.describe())
            assert !table.isDynamic()
            assertEquals(['alice', 'bob'], table.columnNames())
            assertEquals(['foo', 'bar'], table.rowComponents())
        }
    }

    private void rowQueryRunner(row, rowExpect, col=null, colExpect=null) {
        setup()
        def query = [table: TABLE_NAME, rows: rowExpect]
        if(colExpect != null) {
            query.cols = colExpect
        }
        def token = mock(TransactionToken)
        service.getRows(query, token).returns([data: [['a': 'foo']]]).once()
        play {
            assertEquals('a': 'foo', table.getRow(row, col, token))
        }
    }

    @Test
    void testGetRow() {
        rowQueryRunner(['a'], [['a']])
    }

    private void rowsQueryRunner(row, rowExpect, col=null, colExpect=null) {
        setup()
        def query = [table: TABLE_NAME, rows: rowExpect]
        if(colExpect != null) {
            query.cols = colExpect
        }
        def token = mock(TransactionToken)
        service.getRows(query, token).returns([data: [['a': 'foo'], ['b': 'bar']]]).once()
        play {
            assertEquals([['a': 'foo'], ['b': 'bar']], table.getRows(row, col, token))
        }
    }

    @Test
    void testGetRows() {
        tableQueryToServiceQuery.each {
            rowsQueryRunner(it.key, it.value)
            rowsQueryRunner(it.key, it.value, it.key, it.value)
        }
    }

    @Test
    void testGetPartialRows() {
        def query = [table: TABLE_NAME, rows: serviceQuery1, cols: serviceQuery1]
        def token = mock(TransactionToken)
        service.getRows(query, token).returns([data: [['a': 'foo'], ['b': 'bar']]]).once()
        play {
            assertEquals([['a': 'foo'], ['b': 'bar']], table.getPartialRows(tableQuery1, tableQuery1, token))
            shouldFail(MissingMethodException) {
                table.getPartialRows(tableQuery1)
            }
        }
    }

    private void cellQueryRunner(cells, cellsExpect) {
        setup()
        def query = [table: TABLE_NAME, data: cellsExpect]
        def token = mock(TransactionToken)
        service.getCells(query, token).returns([data: [['a': 'foo'], ['b': 'bar']]]).once()
        play {
            assertEquals([['a': 'foo'], ['b': 'bar']], table.getCells(cells, token))
        }

    }

    @Test
    void testGetCells() {
        def nonListParameter = 'a'
        def listParameter = ['a']
        def toCell = { row, col -> [row: row, col: col] }
        def query1 = toCell(nonListParameter, nonListParameter)
        def query2 = toCell(listParameter, listParameter)
        cellQueryRunner(query1, [query2])
        cellQueryRunner([query1], [query2])
        cellQueryRunner(query2, [query2])
        cellQueryRunner([query2], [query2])
    }

    private void rangeTestRunner(query, expected) {
        setup()
        def token = mock(TransactionToken)
        expected['table'] = TABLE_NAME
        def result = [data: expected, next: null]
        service.getRange(expected, token).returns(result).once()
        play {
            assertEquals(new Range(service, result, token), table.getRange(query, token))
        }
    }

    @Test
    void testGetRange() {
        def stringExample = 'a'
        def listExample = [stringExample]
        rangeTestRunner([start: stringExample], [start: listExample])
        rangeTestRunner([end: stringExample], [end: listExample])
        rangeTestRunner([prefix: stringExample], [prefix: listExample])
        rangeTestRunner([cols: stringExample], [cols: listExample])
        rangeTestRunner([cols: listExample, start: listExample, end: listExample], [cols: listExample, start: listExample, end: listExample])
        rangeTestRunner(null, [:])
    }

    def queryize(data) {
        [table: TABLE_NAME, data: data]
    }

    void testDeleteRunner(token, rowsInDatabase, input, output) {
        //have to reimplement part of table.getRows since partial mocks don't work :(
        service.delete(queryize(output), token).once()
        play {
            assertEquals(null, table.delete(input, token))
        }
    }

    @Test
    void testDeleteNamed() {
        service.getMetadata(TABLE_NAME).returns([is_dynamic: false, columns: [[long_name: 'a'], [long_name: 'b']]]).once()
        def rowsInDatabase = [data: [[row: [1], a: 1, b: 2], [row: [2], a: 3, b: 4]]]
        def runTest = { input, output = null ->
            testDeleteRunner(mock(TransactionToken), rowsInDatabase, input, output)
        }
        def firstInput = [row: 1, cols: 'a']
        def firstOutput = [[row: [1], col: 'a']]
        def secondInput = [row: [2], cols: ['a', 'b']]
        def secondOutput = [[row: [2], col: 'a'], [row: [2], col: 'b']]
        runTest(firstInput, firstOutput)
        runTest(secondInput, secondOutput)
        runTest([firstInput, secondInput], firstOutput + secondOutput)
    }

    @Test
    void testDeleteDynamic() {
        service.getMetadata(TABLE_NAME).returns([is_dynamic: true])
        def rowsInDatabase = [data: [
            [row: [1], cols: [[col: ['a'], val: 1], [col: ['b'], val: 2]]],
            [row: [2], cols: [[col: ['c'], val: 3]]]
        ]]
        def runTest = { input, output = null ->
            testDeleteRunner(mock(TransactionToken), rowsInDatabase, input, output)
        }
        def firstInput = [row: 1, cols: 'a']
        def firstOutput = [[row: [1], col: ['a']]]
        def secondInput = [row: [1], cols: ['a', 'b']]
        def secondOutput = [[row: [1], col: ['a']], [row: [1], col: ['b']]]
        def thirdInput = [row: [2], cols: ['c']]
        def thirdOutput = [[row: [2], col: ['c']]]
        runTest(firstInput, firstOutput)
        runTest(secondInput, secondOutput)
        runTest(thirdInput, thirdOutput)
        runTest([secondInput, thirdInput], secondOutput + thirdOutput)
    }

    @Test
    void testPutNamed() {
        final Map DESC = [
            is_dynamic:false,
            columns: [
                [long_name: 'a',
                    value: [type: [fields: [[name: 'value']]]]],
                [long_name: 'b',
                    value: [type: [fields: [[name: 'value']]]]]
            ]
        ]
        service.getMetadata(TABLE_NAME).returns(DESC).once()
        def token = mock(TransactionToken)
        def firstInput = [row: 1, cols: [a: [value: 1]]]
        def firstOutput = [row: [1], a: [value: 1]]
        def secondInput = [row: [2], cols: [a: [value: 1], b: [value: 2]]]
        def secondOutput = [row: [2], a: [value: 1], b: [value: 2]]
        def thirdInput = [row: 3, cols: [a: [value: 1], c: [value: 2]]]
        def fourthInput = [row: 4, cols: [a: [value: 1, fake_field: 0]]]
        service.put(queryize([firstOutput]), token).once()
        service.put(queryize([firstOutput, secondOutput]), token).once()
        play {
            Assert.assertEquals table.put(firstInput, token), null
            Assert.assertEquals table.put([firstInput, secondInput], token), null
            def message = shouldFail(IllegalArgumentException) {
                table.put([firstInput, secondInput, thirdInput], token)
            }
            assertEquals("Column c does not exist", message.message)
            message = shouldFail(IllegalArgumentException) {
                table.put(fourthInput, token)
            }
            assertEquals("The following fields do not exist: [fake_field]", message.message)
        }
    }

    @Test
    void testPutDynamic() {
        service.getMetadata(TABLE_NAME).returns([is_dynamic: true]).once()
        def token = mock(TransactionToken)
        def firstInput = [row: 1, col: 'a', val: 2]
        def firstOutput = [row: [1], col: ['a'], val: 2]
        def secondInput = [row: [2], col: ['b'], val: 3]
        def secondOutput = [row: [2], col: ['b'], val: 3]
        def queryize = { data -> [table: TABLE_NAME, data: data] }
        service.put(queryize([firstOutput]), token).once()
        service.put(queryize([secondOutput]), token).once()
        service.put(queryize([firstOutput, secondOutput]), token).once()
        play {
            assertEquals(null, table.put(firstInput, token))
            assertEquals(null, table.put([secondInput], token))
            assertEquals(null, table.put([firstInput, secondInput], token))
        }
    }
}
