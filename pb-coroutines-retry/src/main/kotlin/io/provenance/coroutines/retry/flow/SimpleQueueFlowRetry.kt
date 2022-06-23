package io.provenance.coroutines.retry.flow

import io.provenance.coroutines.channels.receiveQueued
import io.provenance.coroutines.retry.store.RetryRecord
import java.time.OffsetDateTime
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow

class SimpleChannelFlowRetry<T>(
    private val queue: Channel<RetryRecord<T>> = Channel(capacity = UNLIMITED),
    private val onSuccess: (T) -> Unit = {},
    private val onFailure: (T) -> Unit = {},
    private val block: (T, Int) -> Unit,
) : FlowRetry<T> {
    override suspend fun send(item: T, e: Throwable) {
        queue.send(RetryRecord(item, lastException = e.localizedMessage))
    }

    override suspend fun process(item: T, attempt: Int) {
        block(item, attempt)
    }

    override suspend fun produceNext(attemptRange: IntRange, olderThan: OffsetDateTime, limit: Int): Flow<RetryRecord<T>> {
        return queue.receiveQueued(limit).asFlow()
    }

    override suspend fun onSuccess(item: RetryRecord<T>) {
        onSuccess.invoke(item.data)
    }

    override suspend fun onFailure(item: RetryRecord<T>, e: Throwable) {
        onFailure.invoke(item.data)
    }
}
