package io.provenance.kafka.coroutines.retry.flow

import io.provenance.kafka.coroutines.retry.KafkaRetry
import io.provenance.kafka.coroutines.retry.RetryStrategy
import io.provenance.kafka.coroutines.retry.defaultRetryStrategies
import io.provenance.kafka.coroutines.retry.invert
import io.provenance.kafka.coroutines.retry.store.ConsumerRecordRetry
import java.time.OffsetDateTime
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.toJavaDuration
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import mu.KotlinLogging

@OptIn(ExperimentalTime::class)
fun <K, V> retryKafkaFlow(
    kafkaRetry: KafkaRetry<K, V>,
    retryStrategies: List<RetryStrategy> = defaultRetryStrategies
): Flow<ConsumerRecordRetry<K, V>> = flow {
    val log = KotlinLogging.logger {}
    val strategies = retryStrategies.invert()

    while (true) {
        for (strategy in strategies) {
            val lastAttempted = OffsetDateTime.now().minus(strategy.value.lastAttempted.toJavaDuration())
            val list = kafkaRetry.fetchNext(10, strategy.key, lastAttempted)
            log.debug { "${strategy.value.name} --> Retrying ${list.size} records in group ${strategy.key} last attempted $lastAttempted"}

            for (rec in list) {
                runCatching { kafkaRetry.retry(rec.consumerRecord, rec.attempt) }.fold(
                    onSuccess = {
                        log.debug { "retry succeeded for key:${rec.consumerRecord.key()} after attempt:${rec.attempt}" }
                        kafkaRetry.markComplete(rec.consumerRecord)
                        emit(rec)
                    },
                    onFailure = {
                        log.debug("failed to retry key:${rec.consumerRecord.key()} attempt:${rec.attempt}", it)
                        strategy.value.onFailure("", it)
                        kafkaRetry.markAttempted(rec.consumerRecord)
                    },
                )
            }
        }
        delay(10.seconds)
    }
}