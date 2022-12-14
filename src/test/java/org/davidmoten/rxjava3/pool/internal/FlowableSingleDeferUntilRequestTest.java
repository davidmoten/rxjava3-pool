package org.davidmoten.rxjava3.pool.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.davidmoten.rxjava3.pool.internal.FlowableSingleDeferUntilRequest.SingleSubscription;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subscribers.TestSubscriber;


public class FlowableSingleDeferUntilRequestTest {

    @Test
    public void testIsDeferred() {
        AtomicBoolean a = new AtomicBoolean();
        Single<Integer> s = Single.fromCallable(() -> {
            a.set(true);
            return 1;
        });
        TestSubscriber<Integer> ts = new FlowableSingleDeferUntilRequest<Integer>(s) //
                .test(0);
        assertFalse(a.get());
        ts.requestMore(1);
        assertTrue(a.get());
        ts.assertValue(1);
        ts.assertComplete();
    }

    @Test
    public void testErrorDeferred() {
        Single<Integer> s = Single.fromCallable(() -> {
            throw new RuntimeException("boo");
        });
        TestSubscriber<Integer> ts = new FlowableSingleDeferUntilRequest<Integer>(s) //
                .test();
        ts.assertError(RuntimeException.class);
        ts.assertNoValues();
    }

    @Test
    public void testCancelBeforeRequest() {
        Single<Integer> s = Single.fromCallable(() -> {
            return 1;
        });
        TestSubscriber<Integer> ts = new FlowableSingleDeferUntilRequest<Integer>(s) //
                .test(0);
        ts.cancel();
        ts.assertNoValues();
        ts.assertNotComplete();
        ts.assertNoErrors();
        ts.cancel();
    }
    
    @Test
    public void testSubscribeTwice() {
        Single<Integer> s = Single.fromCallable(() -> {
            return 1;
        });
        SingleSubscription<Integer> sub = new SingleSubscription<Integer>(s, new Subscriber<Integer>() {

            @Override
            public void onSubscribe(Subscription s) {
                
            }

            @Override
            public void onNext(Integer t) {
                
            }

            @Override
            public void onError(Throwable t) {
                
            }

            @Override
            public void onComplete() {
                
            }});
        sub.request(0);
        sub.request(1);
        sub.request(1);
        AtomicBoolean b = new AtomicBoolean();
        sub.onSubscribe(new Disposable() {

            @Override
            public void dispose() {
                b.set(true);
            }

            @Override
            public boolean isDisposed() {
                // TODO Auto-generated method stub
                return false;
            }});
        assertTrue(b.get());
        sub.request(1);
    }

}
