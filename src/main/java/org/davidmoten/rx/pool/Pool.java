package org.davidmoten.rx.pool;

import io.reactivex.rxjava3.core.Single;

public interface Pool<T> extends AutoCloseable {

    Single<Member<T>> member();

}