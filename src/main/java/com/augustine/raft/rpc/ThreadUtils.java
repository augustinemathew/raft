package com.augustine.raft.rpc;

import lombok.NonNull;

public class ThreadUtils {

    public static String getMonitorOwner(@NonNull Object obj) {
        if (Thread.holdsLock(obj)) return Thread.currentThread().getName();
        for (java.lang.management.ThreadInfo ti :
                java.lang.management.ManagementFactory.getThreadMXBean()
                        .dumpAllThreads(true, false)) {
            for (java.lang.management.MonitorInfo mi : ti.getLockedMonitors()) {
                if (mi.getIdentityHashCode() == System.identityHashCode(obj)) {
                    return ti.getThreadName();
                }
            }
        }
        return "";
    }
}
