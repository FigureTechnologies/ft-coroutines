package io.provenance.kafka.coroutines.retry.store

import java.time.OffsetDateTime
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader

open class RetryRecord<T>(
    open val data: T,
    open val attempt: Int,
    open val lastAttempted: OffsetDateTime,
    open val lastException: String,
) {
    fun copy(
        data: T = this.data,
        attempt: Int = this.attempt,
        lastAttempted: OffsetDateTime = this.lastAttempted,
        lastException: String = this.lastException
    ) = RetryRecord(data, attempt, lastAttempted, lastException)

    override fun toString(): String = "RetryRecord(attempt:$attempt lastAttempted:$lastAttempted data:$data reason:$lastException)"
}

private fun Headers.addOrUpdate(header: Header): Headers {
    val h = find { it.key() == header.key() }
    if (h == null) {
        add(header)
    } else {
        remove(header.key())
        add(header.key(), header.value())
    }
    return this
}

fun <K, V> ConsumerRecord<K, V>.setHeader(key: String, value: ByteArray): ConsumerRecord<K, V> = apply {
    headers().addOrUpdate(RecordHeader(key, value))
}
