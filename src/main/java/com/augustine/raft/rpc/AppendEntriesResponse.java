package com.augustine.raft.rpc;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder
@AllArgsConstructor
public class AppendEntriesResponse{
    private final long term;
    private final boolean succeeded;
    private final long lastLogIndexOnServer;
    private final ConflictDetails conflictDetails;

    @Getter
    @Builder
    @AllArgsConstructor
    public static class ConflictDetails {
        private final long conflictingTerm;
        private final long logIndexOfFirstEntryInConflictingTerm;
    }

    @Override
    public String toString() {
        return "AppendEntriesResponse{" + "term=" + term + ", succeeded=" + succeeded + ", lastLogIndexOnServer=" +
            lastLogIndexOnServer + ", conflictDetails=" + conflictDetails + '}';
    }
}
