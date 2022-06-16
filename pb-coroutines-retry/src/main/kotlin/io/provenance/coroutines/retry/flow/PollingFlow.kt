package io.provenance.coroutines.retry.flow

import kotlin.time.Duration
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow

/**
 * Create a polling [Flow].
 *
 * @param pollInterval The interval to wait between calls to [block].
 * @param block Lambda returning the next element to be emitted, or null if nothing is ready.
 */
@OptIn(ExperimentalCoroutinesApi::class)
inline fun <T> pollingFlow(pollInterval: Duration, crossinline block: suspend ProducerScope<T>.() -> Unit): Flow<T> = channelFlow {
    while (!isClosedForSend) {
        block(this)
        delay(pollInterval.inWholeMilliseconds)
    }
}
