package com.augustine.raft;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class CompletableFutures {

    public static <T> void cancelAll(@NonNull Collection<CompletableFuture<T>> futures, boolean mayInterruptIfRunning) {
        for(CompletableFuture future : futures) {
            try {
                future.cancel(mayInterruptIfRunning);
            }catch (CancellationException ce){
            }catch (Exception e){
                log.error("Error cancelling future", e);
            }
        }
    }
}
