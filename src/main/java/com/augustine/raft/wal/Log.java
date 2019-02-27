package com.augustine.raft.wal;

import java.util.List;
import java.util.Optional;

public interface Log {

    long getFirstEntryIndex();

    long getLastEntryIndex();

    void appendLogEntries(List<LogEntry> entries);

    List<LogEntry> getLogEntries(long beginIndex, long endIndexInclusive);

    List<LogEntry> getLogEntries(long beginIndex);

    LogEntry getLogEntry(long logIndex);

    boolean isEmpty();

    void removeEntriesStartingFromIndex(long logIndex);

    void removeEntriesUntil(long endIndex, LogEntry seedEntry);
}
