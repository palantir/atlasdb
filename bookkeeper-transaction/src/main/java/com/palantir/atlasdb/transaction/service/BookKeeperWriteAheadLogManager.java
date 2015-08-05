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
package com.palantir.atlasdb.transaction.service;

import java.io.IOException;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.KeeperException;

import com.palantir.common.base.Throwables;


public class BookKeeperWriteAheadLogManager implements WriteAheadLogManager {
    private BookKeeper bookKeeper;

    private BookKeeperWriteAheadLogManager(BookKeeper bookKeeper) {
        this.bookKeeper = bookKeeper;
    }

    public static BookKeeperWriteAheadLogManager create(String servers) {
        try {
            BookKeeper bookKeeper = new BookKeeper(servers);
            return new BookKeeperWriteAheadLogManager(bookKeeper);
        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        } catch (KeeperException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public WriteAheadLog create() {
        try {
            return new BookKeeperWriteAheadLog(bookKeeper.createLedger(DigestType.CRC32, new byte[0]), false);
        } catch (BKException e) {
            throw Throwables.throwUncheckedException(e);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public WriteAheadLog retrieve(long logId) {
        try {
            LedgerHandle handle = bookKeeper.openLedger(logId, DigestType.CRC32, new byte[0]);
            handle.close();
            return new BookKeeperWriteAheadLog(handle, true);
        } catch (BKException e) {
            throw Throwables.throwUncheckedException(e);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }
}
