package tech.figure.coroutines.retry.store

import java.time.OffsetDateTime

open class RetryRecord<T>(
    var data: T,
    var attempt: Int = 0,
    var lastAttempted: OffsetDateTime = OffsetDateTime.now(),
    var lastException: String = ""
) {
    override fun toString(): String = "RetryRecord(attempt:$attempt Exception:$lastException lastAttempted:$lastAttempted data:$data)"
}
