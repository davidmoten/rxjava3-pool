package org.davidmoten.rxjava3.pool;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.core.Scheduler.Worker;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.internal.disposables.DisposableHelper;

public class MemberSingleTest {

    @Test
    public void testHealthCheck() {

        long now = 10000;
        NonBlockingPool<Integer> pool = NonBlockingPool.factory(() -> 1) //
                .scheduler(new Scheduler() {

                    final Worker WORKER = new ImmediateThinWorker();

                    @Override
                    public @NonNull Worker createWorker() {
                        return WORKER;
                    }

                    @Override
                    public long now(@NonNull TimeUnit unit) {
                        return unit.toMillis(1) * now;
                    }

                }).build();
        MemberSingle<Integer> member = new MemberSingle<>(pool);
        DecoratingMember<Integer> m = new DecoratingMember<>(1, (x, y) -> x, member);

        assertFalse(MemberSingle.shouldPerformHealthCheck(m, 0, 1000));
        // set lastCheckTime to now
        m.markAsChecked();
        assertFalse(MemberSingle.shouldPerformHealthCheck(m, 2000, now + 1000));
        assertTrue(MemberSingle.shouldPerformHealthCheck(m, 500, now + 1000));
    }

    static final class ImmediateThinWorker extends Worker {
        static final Disposable DISPOSED;

        static {
            DISPOSED = Disposable.empty();
            DISPOSED.dispose();
        }

        @Override
        public void dispose() {
            // This worker is always stateless and won't track tasks
        }

        @Override
        public boolean isDisposed() {
            return false; // dispose() has no effect
        }

        @NonNull
        @Override
        public Disposable schedule(@NonNull Runnable run) {
            run.run();
            return DisposableHelper.DISPOSED;
        }

        @NonNull
        @Override
        public Disposable schedule(@NonNull Runnable run, long delay, @NonNull TimeUnit unit) {
            throw new UnsupportedOperationException("This scheduler doesn't support delayed execution");
        }

        @NonNull
        @Override
        public Disposable schedulePeriodically(@NonNull Runnable run, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException("This scheduler doesn't support periodic execution");
        }
    }

}
