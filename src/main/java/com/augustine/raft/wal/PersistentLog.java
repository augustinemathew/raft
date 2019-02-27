package com.augustine.raft.wal;

import com.augustine.raft.RaftConfiguration;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.GetOp;
import org.lmdbjava.SeekOp;
import org.lmdbjava.Txn;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentLog implements Log, AutoCloseable {
    private final Env<ByteBuffer> dbEnvironment;
    private final Dbi<ByteBuffer> logDb;
    private final ReentrantReadWriteLock rwLock;

    public PersistentLog(@NonNull String filePath){
        this.rwLock = new ReentrantReadWriteLock(false);
        if(!Files.exists(Paths.get(filePath))){
            new File(filePath).mkdirs();
        }
        // We always need an Env. An Env owns a physical on-disk storage file. One
        // Env can store many different databases (ie sorted maps).

        this.dbEnvironment = Env.create()
                // LMDB also needs to know how large our DB might be. Over-estimating is OK.
                .setMapSize(100_485_760)
                .setMaxReaders(1024)
                // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
                .setMaxDbs(1)
                // Now let's open the Env. The same path can be concurrently opened and
                // used in different processes, but do not open the same path twice in
                // the same process at the same time.
                .open(new File(filePath));
        this.logDb = dbEnvironment.openDbi("RaftLog", DbiFlags.MDB_CREATE);
        this.checkIfLogEmptyAndPutEmpty();
    }

    private void checkIfLogEmptyAndPutEmpty(){
        long first =getEntryIndexUnsafe(true);
        long last =getEntryIndexUnsafe(false);
        if(first == last && first == -1){
            List<LogEntry> entries = new ArrayList<>();
            entries.add(LogEntry.builder()
                    .type(LogEntryType.NORMAL)
                    .term(0)
                    .lsn(0)
                    .array(new byte[]{})
                    .build());
            appendEntriesUnsafe(entries);
        }
    }

    @Override
    public long getFirstEntryIndex()  {
        return executeWithReadLock(this::getFirstEntryIndexUnsafe);
    }

    @Override
    public long getLastEntryIndex()  {
        return executeWithReadLock(this::getLastEntryIndexUnsafe);
    }

    @Override
    public void appendLogEntries(@NonNull List<LogEntry> entries) {
        executeWithWriteLock(()->{
            appendEntriesUnsafe(entries);
            return true;
        });
    }

    private void appendEntriesUnsafe(@NonNull List<LogEntry> entries) {
        long lastLsn = this.getLastEntryIndexUnsafe();
        try(Txn<ByteBuffer> writeTxn = this.dbEnvironment.txnWrite()){
            for (LogEntry logEntry: entries) {
                logEntry = logEntry.toBuilder()
                        .lsn(++lastLsn)
                        .build();

                this.logDb.put(writeTxn, toByteBuffer(lastLsn), logEntry.toByteBuffer());
            }
            writeTxn.commit();
        }
    }

    private long getFirstEntryIndexUnsafe() {
        return getEntryIndexUnsafe(true);
    }

    private long getLastEntryIndexUnsafe() {
        return getEntryIndexUnsafe(false);
    }

    private long getEntryIndexUnsafe(boolean first){
        try(Txn<ByteBuffer> txnRead = this.dbEnvironment.txnRead()){
            try(Cursor<ByteBuffer> cursor=this.logDb.openCursor(txnRead)) {
                if(!cursor.seek(first? SeekOp.MDB_FIRST : SeekOp.MDB_LAST)){
                    return -1;
                }
                return fromByteBuffer(cursor.key());
            }
        }
    }

    @Override
    public List<LogEntry> getLogEntries(long beginIndex, long endIndexInclusive) {
        return executeWithReadLock(()->{
            List<LogEntry> logEntries = new ArrayList<>();
            try(Txn<ByteBuffer> readTxn = this.dbEnvironment.txnRead()){
                try(Cursor<ByteBuffer> cursor = this.logDb.openCursor(readTxn)){
                    if(cursor.get(toByteBuffer(beginIndex),GetOp.MDB_SET_KEY)) {
                        do  {
                            if (fromByteBuffer(cursor.key()) > endIndexInclusive) {
                                break;
                            }
                            logEntries.add(LogEntry.LogEntryBuilder.fromByteBuffer(cursor.val()));
                        }while (cursor.next());
                    }else{
                        throw new IllegalArgumentException("Log index "+beginIndex+" does't exist");
                    }
                }
                readTxn.commit();
            }
            return logEntries;
        });
    }


    @Override
    public List<LogEntry> getLogEntries(long beginIndex) {
        return this.getLogEntries(beginIndex,Long.MAX_VALUE);
    }

    @Override
    public LogEntry getLogEntry(long logIndex) {
        return executeWithReadLock(()->{
            try(Txn<ByteBuffer> readTxn = this.dbEnvironment.txnRead()){
                ByteBuffer val = this.logDb.get(readTxn, toByteBuffer(logIndex));
                if(val == null){
                    throw new IllegalArgumentException("Log index does not exist " + logIndex );
                }
                return LogEntry.LogEntryBuilder.fromByteBuffer(val);
            }
        });
    }

    @Override
    public boolean isEmpty() {
        return executeWithReadLock(()-> getFirstEntryIndex() == getLastEntryIndex());
    }

    @Override
    public synchronized void removeEntriesStartingFromIndex(long logIndex) {
        //This is to ensure that we always have 0 log index no matter what.
        //0 log index contains an entry for term 0 and log index 0. It tells
        //the raft server that it is an empty replica

        //Fix this to account for snapshot entry
        final long indexToStartDeleting = logIndex;
        executeWithWriteLock(()->{

            try(Txn<ByteBuffer> deleteTxn = this.dbEnvironment.txnWrite()){
               try(Cursor<ByteBuffer> cursor = this.logDb.openCursor(deleteTxn)) {
                   if (cursor.get(toByteBuffer(indexToStartDeleting), GetOp.MDB_SET_KEY)) {
                       cursor.delete();
                       while (cursor.next()) {
                           cursor.delete();
                       }
                   }
               }
               deleteTxn.commit();
               checkIfLogEmptyAndPutEmpty();
            }
            return true;
        });
    }

    @Override
    public synchronized void removeEntriesUntil(long endIndex, LogEntry seedEntry) {
        long indexToStartDeleting = getFirstEntryIndex();
        executeWithWriteLock(()->{

            try(Txn<ByteBuffer> deleteTxn = this.dbEnvironment.txnWrite()){
                try(Cursor<ByteBuffer> cursor = this.logDb.openCursor(deleteTxn)) {
                    if (cursor.get(toByteBuffer(indexToStartDeleting), GetOp.MDB_SET_KEY)) {
                        cursor.delete();
                        while (cursor.next() && fromByteBuffer(cursor.key())<=endIndex) {
                            cursor.delete();
                        }
                    }
                }
                LogEntry entryToStore = seedEntry.toBuilder()
                        .lsn(endIndex)
                        .build();
                this.logDb.put(deleteTxn,toByteBuffer(endIndex), entryToStore.toByteBuffer());
                deleteTxn.commit();
                checkIfLogEmptyAndPutEmpty();
            }
            return true;
        });
    }

    @Override
    public void close() {
        Lock writeLock = this.rwLock.writeLock();
        writeLock.lock();
        try {
            this.logDb.close();
            this.dbEnvironment.close();
        }finally {
            writeLock.unlock();
        }
    }



    private <T> T executeWithWriteLock(Callable<T> callable){
        return executeWithLock(callable, this.rwLock.writeLock());
    }

    private <T> T executeWithReadLock(Callable<T> callable){
        return executeWithLock(callable, this.rwLock.readLock());
    }

    private <T> T executeWithLock(Callable<T> callable, Lock lock){
        Callable<T> wrapped = ()-> {
            lock.lock();
            try {
                return callable.call();
            } finally {
                lock.unlock();
            }
        };
        try {
            return wrapped.call();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private ByteBuffer toByteBuffer(long longVal){
        ByteBuffer buf = ByteBuffer.allocateDirect(80);
        buf.putLong(longVal).flip();
        return buf;
    }

    private long fromByteBuffer(ByteBuffer key) {
        return key.getLong();
    }

}
