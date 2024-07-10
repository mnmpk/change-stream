package com.example.demo.service;

import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

public class ChangeStreamSubscriber<T> implements Subscriber<ChangeStreamDocument<T>> {
    private Subscription subscription;
    private Consumer<ChangeStreamDocument<T>> onNext;
    private Consumer<Throwable> onError;

    public ChangeStreamSubscriber(Consumer<ChangeStreamDocument<T>> onNext, Consumer<Throwable> onError) {
        this.onNext = onNext;
        this.onError = onError;
    }
    @Override
    public void onComplete() {
        this.subscription.cancel();
    }

    @Override
    public void onError(Throwable t) {
        this.onError.accept(t);
    }

    @Override
    public void onNext(ChangeStreamDocument<T> t) {
        this.onNext.accept(t);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(Long.MAX_VALUE);
    }

}
