package tech.figure.kafka.context.extensions

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import tech.figure.kafka.records.KafkaRecord
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
 * Fetches a valid Instant representation for the Kafka record, with regard to the valid timestamp types in the
 * encountered data.
 */
fun <K, V> ConsumerRecord<K, V>.timestampToInstant(): Instant = getKafkaTimestampInstant(
    timestampType = timestampType(),
    millis = timestamp(),
)

/**
 * Fetches a valid OffsetDateTime representation for the Kafka record, with regard to the valid timestamp types in the
 * encountered data.
 */
fun <K, V> ConsumerRecord<K, V>.timestampToOffsetDateTime(
    zone: ZoneId = ZoneOffset.systemDefault(),
): OffsetDateTime = OffsetDateTime.ofInstant(this.timestampToInstant(), zone)

/**
 * Fetches a valid Instant representation for the Kafka record, with regard to the valid timestamp types in the
 * encountered data.
 */
fun <K, V> KafkaRecord<K, V>.timestampToInstant(): Instant = getKafkaTimestampInstant(
    timestampType = timestampType,
    millis = timestamp,
)

/**
 * Fetches a valid OffsetDateTime representation for the Kafka record, with regard to the valid timestamp types in the
 * encountered data.
 */
fun <K, V> KafkaRecord<K, V>.timestampToOffsetDateTime(
    zone: ZoneId = ZoneOffset.systemDefault(),
): OffsetDateTime = OffsetDateTime.ofInstant(this.timestampToInstant(), zone)

private fun getKafkaTimestampInstant(
    timestampType: TimestampType,
    millis: Long,
): Instant = if (timestampType in VALID_ODT_TIMESTAMP_TYPES) {
    Instant.ofEpochMilli(millis)
} else {
    error("Unexpected timestamp type [$timestampType] with millis [$millis]")
}
