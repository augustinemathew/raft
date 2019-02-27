package com.augustine.raft.wal;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class LogConsistencyChecker {

    public static void checkLogs(@NonNull Log log1,
                                 @NonNull Log log2){
        List<LogEntry> entries1 = log1.getLogEntries(0);
        List<LogEntry> entries2 = log2.getLogEntries(0);
        for(int i=0; i<Math.min(entries1.size(),entries2.size()); i++){
            if (entries1.get(i).getTerm() == entries2.get(i).getTerm() &&
                entries1.get(i).getLsn() == entries2.get(i).getLsn() &&
                Arrays.equals(entries1.get(i).getArray(), entries2.get(i).getArray())){

            }else{
                throw new RuntimeException(" Mismatch at " + i);
            }
        }
    }
}
