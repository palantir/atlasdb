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
