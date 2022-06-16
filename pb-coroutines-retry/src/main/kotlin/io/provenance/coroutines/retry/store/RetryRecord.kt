package io.provenance.coroutines.retry.store

import java.time.OffsetDateTime

open class RetryRecord<T>(
    val data: T,
    val attempt: Int = 0,
    val lastAttempted: OffsetDateTime = OffsetDateTime.now(),
) {
    fun copy(
        data: T = this.data,
        attempt: Int = this.attempt,
        lastAttempted: OffsetDateTime = this.lastAttempted
    ) = RetryRecord(data, attempt, lastAttempted)

    override fun toString(): String = "RetryRecord(attempt:$attempt lastAttempted:$lastAttempted data:$data)"
}
