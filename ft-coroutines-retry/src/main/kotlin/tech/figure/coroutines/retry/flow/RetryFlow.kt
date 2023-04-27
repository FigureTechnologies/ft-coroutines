package tech.figure.coroutines.retry.flow

import java.time.OffsetDateTime
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onStart
import mu.KotlinLogging
import tech.figure.coroutines.retry.RetryStrategy
import tech.figure.coroutines.retry.defaultRetryStrategies
import tech.figure.coroutines.retry.invert
import tech.figure.coroutines.retry.store.RetryRecord
import tech.figure.coroutines.tryMap

internal val DEFAULT_RETRY_INTERVAL = 10.seconds

/**
 * Create a retrying [Flow].
 *
 * Using [FlowRetry.produceNext] as the data source, feed retry records into [FlowRetry.process].
 * * If successful, route through [FlowRetry.onSuccess].
 * * If failed, route through [FlowRetry.onFailure].
 *
 * Once a record is successfully processed, emit the data element out to the flow.
 */
@OptIn(ExperimentalCoroutinesApi::class)
fun <T> retryFlow(
    flowRetry: FlowRetry<T>,
    retryInterval: Duration = DEFAULT_RETRY_INTERVAL,
    batchSize: Int = DEFAULT_FETCH_LIMIT,
    retryStrategies: List<RetryStrategy> = defaultRetryStrategies,
): Flow<T> {
    val log = KotlinLogging.logger {}
    val strategies = retryStrategies.invert()

    log.info { "initializing polling retry flow ${flowRetry.javaClass.name}" }
    return pollingFlow(retryInterval) {
        if (!flowRetry.hasNext()) {
            return@pollingFlow
        }

        for (strategy in strategies) {
            val attemptedBefore = OffsetDateTime.now().minus(strategy.value.lastAttempted.toJavaDuration())

            val onFailure: suspend (RetryRecord<T>, Throwable) -> Unit = { rec, it ->
                strategy.value.onFailure("", it)
                flowRetry.onFailure(rec, it)
            }

            flowRetry.produceNext(strategy.key, attemptedBefore, batchSize)
                .onStart {
                    log.trace { "${strategy.value.name} --> Retrying records in group:${strategy.key} lastAttempted:$attemptedBefore" }
                }
                .tryMap(onFailure) {
                    log.debug { "retry processing attempt:${it.attempt} rec:${it.data}" }
                    flowRetry.process(it.data, it.attempt)

                    log.debug { "retry succeeded on attempt:${it.attempt} rec:${it.data}" }
                    flowRetry.onSuccess(it)
                }
                .collect()
        }
    }
}
