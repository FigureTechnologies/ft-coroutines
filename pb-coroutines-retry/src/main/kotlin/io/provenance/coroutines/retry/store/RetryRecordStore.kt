package io.provenance.coroutines.retry.store

import java.time.OffsetDateTime

interface RetryRecordStore<T> {
    suspend fun select(attemptRange: IntRange, lastAttempted: OffsetDateTime): List<RetryRecord<T>>
    suspend fun getOne(item: T): RetryRecord<T>?
    suspend fun putOne(item: T, lastException: Throwable? = null, mutator: RetryRecord<T>.() -> Unit)
    suspend fun remove(item: T)
}
