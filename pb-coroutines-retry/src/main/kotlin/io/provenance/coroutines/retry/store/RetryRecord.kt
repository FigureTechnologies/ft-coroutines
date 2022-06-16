package io.provenance.coroutines.retry.store

import java.time.OffsetDateTime

open class RetryRecord<T>(
    val data: T,
    val attempt: Int = 0,
    val lastAttempted: OffsetDateTime = OffsetDateTime.now(),
    val lastException: String
) {
    fun copy(
        data: T = this.data,
        attempt: Int = this.attempt,
        lastAttempted: OffsetDateTime = this.lastAttempted,
        newException: String = this.lastException
    ) = RetryRecord(data, attempt, lastAttempted, newException)

    override fun toString(): String = "RetryRecord(attempt:$attempt Exception:$lastException lastAttempted:$lastAttempted data:$data)"
}
