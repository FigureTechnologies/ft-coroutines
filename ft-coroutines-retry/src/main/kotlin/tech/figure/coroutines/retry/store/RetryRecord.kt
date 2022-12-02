package tech.figure.coroutines.retry.store

import java.time.OffsetDateTime

open class RetryRecord<T>(
    var data: T,
    var attempt: Int = 0,
    var lastAttempted: OffsetDateTime = OffsetDateTime.now(),
    var lastException: String = "",
    var collection: String = "",
) {
    override fun toString(): String =
        "RetryRecord(attempt:$attempt exception:$lastException lastAttempted:$lastAttempted data:$data collection:$collection)"
}
