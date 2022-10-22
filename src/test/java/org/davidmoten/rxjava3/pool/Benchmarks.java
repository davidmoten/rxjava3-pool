package org.davidmoten.rxjava3.pool;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class Benchmarks {
    
    private static final Logger log = LoggerFactory.getLogger(Benchmarks.class);
    
    public static void main(String[] args) throws Exception {
        int poolSize = 4;
        Scheduler scheduler = Schedulers.from(Executors.newFixedThreadPool(poolSize));
        @NonNull
        Scheduler observeOn = Schedulers.from(Executors.newFixedThreadPool(1));
        for (int i = 0; i< 1;i++) {
            log.debug("--------------");
            log.debug("--------------");
            // attempt to get coverage of x.activeCount > 0 expression in tryEmit
            // has not been successful but covers the other statements in tryEmit
            AtomicLong count = new AtomicLong();
            AtomicLong disposed = new AtomicLong();

            try (Pool<Long> pool = NonBlockingPool //
                    .factory(() -> count.incrementAndGet()) //
                    .healthCheck(n -> true) //
                    .maxSize(poolSize) //
                    .maxIdleTime(1, TimeUnit.MINUTES) //
                    .disposer(n -> disposed.incrementAndGet()) //
                    .build()) {
                long n = Long.parseLong(System.getProperty("n", "100"));
                long[] c = new long[1];

                Flowable //
                        .rangeLong(0, n) //
                        .flatMapSingle(x -> pool //
                                .member() //
                                .subscribeOn(scheduler) //
                                , false, poolSize) //
                        .doOnNext(x -> c[0]++) //
                        // have to keep the observeOn buffer small so members don't get buffered
                        // and not checked in
                        .observeOn(observeOn, false, 1) //
                        .doOnNext(member -> member.checkin()) //
                        .timeout(10, TimeUnit.SECONDS) //
                        .count() //
                        .blockingGet();
                // note that the last member in particular may
                assertEquals(c[0], n);
                assertEquals(0, disposed.get());
            }
            assertEquals(poolSize, disposed.get());
        }
    }

}
