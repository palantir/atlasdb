/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.SocketTimeoutException;
import java.util.NoSuchElementException;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

public class CassandraRequestExceptionHandlerTest {

    @Test
    public void testIsConnectionException() {
        Assert.assertFalse(CassandraRequestExceptionHandler.isConnectionException(new TimedOutException()));
        Assert.assertFalse(CassandraRequestExceptionHandler.isConnectionException(new TTransportException()));
        Assert.assertTrue(CassandraRequestExceptionHandler.isConnectionException(new TTransportException(
                new SocketTimeoutException())));
    }

    @Test
    public void testIsRetriableException() {
        Assert.assertTrue(CassandraRequestExceptionHandler.isRetriableException(new TimedOutException()));
        Assert.assertTrue(CassandraRequestExceptionHandler.isRetriableException(new TTransportException()));
        Assert.assertTrue(CassandraRequestExceptionHandler
                .isRetriableException(new TTransportException(new SocketTimeoutException())));
    }

    @Test
    public void testIsRetriableWithBackoffException() {
        Assert.assertTrue(CassandraRequestExceptionHandler
                .isRetriableWithBackoffException(new NoSuchElementException()));
        Assert.assertTrue(CassandraRequestExceptionHandler
                .isRetriableWithBackoffException(new UnavailableException()));
        Assert.assertTrue(CassandraRequestExceptionHandler
                .isRetriableWithBackoffException(new TTransportException(new SocketTimeoutException())));
        Assert.assertTrue(CassandraRequestExceptionHandler
                .isRetriableWithBackoffException(new TTransportException(new UnavailableException())));
    }

    @Test
    public void testIsFastFailoverException() {
        Assert.assertFalse(CassandraRequestExceptionHandler
                .isRetriableWithBackoffException(new InvalidRequestException()));
        Assert.assertFalse(CassandraRequestExceptionHandler.isRetriableException(new InvalidRequestException()));
        Assert.assertFalse(CassandraRequestExceptionHandler.isConnectionException(new InvalidRequestException()));
        Assert.assertTrue(CassandraRequestExceptionHandler.isFastFailoverException(new InvalidRequestException()));
    }
}
