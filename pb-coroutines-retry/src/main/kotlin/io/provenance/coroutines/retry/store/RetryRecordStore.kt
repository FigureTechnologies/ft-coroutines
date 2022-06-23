package io.provenance.coroutines.retry.store

import io.provenance.coroutines.retry.flow.DEFAULT_FETCH_LIMIT
import java.time.OffsetDateTime

interface RetryRecordStore<T> {
    suspend fun select(attemptRange: IntRange, lastAttempted: OffsetDateTime, limit: Int = DEFAULT_FETCH_LIMIT): List<RetryRecord<T>>
    suspend fun getOne(item: T): RetryRecord<T>?
    suspend fun putOne(item: T, lastException: Throwable? = null, mutator: RetryRecord<T>.() -> Unit)
    suspend fun remove(item: T)
}
