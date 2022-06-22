package io.provenance.coroutines.retry.store

import java.time.OffsetDateTime

open class RetryRecord<T>(
    var data: T,
    var attempt: Int = 0,
    var lastAttempted: OffsetDateTime = OffsetDateTime.now(),
    var lastException: String = ""
) {
    fun copy(
        data: T = this.data,
        attempt: Int = this.attempt,
        lastAttempted: OffsetDateTime = this.lastAttempted,
        newException: String = this.lastException
    ) = RetryRecord(data, attempt, lastAttempted, newException)

    override fun toString(): String = "RetryRecord(attempt:$attempt Exception:$lastException lastAttempted:$lastAttempted data:$data)"
}
