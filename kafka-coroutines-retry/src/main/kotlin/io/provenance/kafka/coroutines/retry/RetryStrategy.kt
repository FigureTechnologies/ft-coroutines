package io.provenance.kafka.coroutines.retry

import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import mu.KotlinLogging

private fun <A, B> noop2(): (A, B) -> Unit = { _, _ -> }

private fun <A, B : Throwable> logFailure(): (A, B) -> Unit {
    val log = KotlinLogging.logger {}
    return { it, e ->
        log.error("failed to process $it", e)
    }
}

data class RetryStrategy(val lastAttempted: Duration, val attempts: Int, val onFailure: (String, Throwable) -> Unit = logFailure()) {
    val name: String = "${lastAttempted.toString().lowercase()}-${attempts}-times"
}

val defaultRetryStrategies = listOf(
    RetryStrategy(10.seconds, 3),
    RetryStrategy(5.minutes, 5),
    RetryStrategy(1.hours, 5),
    RetryStrategy(3.hours, 2),
    RetryStrategy(1.days, 8),
    RetryStrategy(1.days, Int.MAX_VALUE)
)

internal fun List<RetryStrategy>.invert(): Map<IntRange, RetryStrategy> {
    // Convert to list of ranges.
    var idx = 0
    val map = mutableMapOf<IntRange, RetryStrategy>()
    for (retry in this) {
        map.putIfAbsent(idx until (retry.attempts + idx), retry)
        idx += retry.attempts
    }
    return map
}
