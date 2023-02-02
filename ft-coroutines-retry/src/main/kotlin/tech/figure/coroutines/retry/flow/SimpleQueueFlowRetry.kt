package tech.figure.coroutines.retry.flow

import tech.figure.coroutines.channels.receiveQueued
import tech.figure.coroutines.retry.store.RetryRecord
import java.time.OffsetDateTime
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow

open class SimpleChannelFlowRetry<T>(
    private val queue: Channel<RetryRecord<T>> = Channel(capacity = UNLIMITED),
    private val onSuccess: (item: T) -> Unit = {},
    private val onFailure: (item: T) -> Unit = {},
    private val block: (item: T, attempt: Int) -> Unit,
) : FlowRetry<T> {
    @OptIn(ExperimentalCoroutinesApi::class)
    override suspend fun hasNext(): Boolean = !queue.isEmpty

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
