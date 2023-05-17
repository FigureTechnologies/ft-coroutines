package tech.figure.kafka.context.extensions

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset

/**
 * All valid timestamp type values that can be produced by Kafka to indicate a proper record timestamp.
 */
private val VALID_ODT_TIMESTAMP_TYPES: Set<TimestampType> = setOf(
    TimestampType.CREATE_TIME,
    TimestampType.LOG_APPEND_TIME,
)

/**
 * Fetches a valid OffsetDateTime representation for the Kafka record, with regard to the valid timestamp types in the
 * encountered data.
 */
fun <K, V> ConsumerRecord<K, V>.timestampTz(
    zone: ZoneId = ZoneOffset.systemDefault(),
): OffsetDateTime = timestampType().let { timestampType ->
    if (timestampType in VALID_ODT_TIMESTAMP_TYPES) {
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp()), zone)
    } else {
        error("Unexpected timestamp type [$timestampType] in record at offset [${offset()}]")
    }
}
