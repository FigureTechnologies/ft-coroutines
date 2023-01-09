package tech.figure.coroutines.flows

import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import mu.KotlinLogging

/**
 * Returns a flow of lists each not exceeding the given [maxSize]. The last list in the resulting
 * flow may have fewer elements than the given [maxSize].
 *
 * @param maxSize the number of elements to take in each list, must be positive and can be greater
 * than the number of elements in this flow. it is non-empty.
 * @param timeout the maximum time period to wait for data to come in through the flow before emitting
 * the current buffered data.
 */
@OptIn(ExperimentalCoroutinesApi::class, ObsoleteCoroutinesApi::class)
fun <T> Flow<T>.chunked(maxSize: Int, timeout: Duration = 1000.milliseconds): Flow<List<T>> {
    val log = KotlinLogging.logger {}
    val buffer = Channel<T>(maxSize)
    return channelFlow {
        coroutineScope {
            launch { this@chunked.collect { buffer.send(it) } }
            launch {
                while (!buffer.isClosedForReceive) {
                    var lastSeenAt = java.time.Instant.now()
                    fun elapsed(c: java.time.Instant) =
                        (java.time.Instant.now().toEpochMilli() - c.toEpochMilli()).milliseconds

                    val chunk = mutableListOf<T>()
                    while (chunk.size < maxSize && elapsed(lastSeenAt) < timeout) {
                        val next =
                            select<T?> {
                                buffer.onReceive {
                                    return@onReceive it
                                }
                                ticker(timeout.inWholeMilliseconds).onReceive {
                                    return@onReceive null
                                }
                            }

                        if (next != null) {
                            lastSeenAt = java.time.Instant.now()
                            chunk += next
                        }
                    }

                    log.trace { "size(${chunk.size}/$maxSize) or timeout(${elapsed(lastSeenAt)} > $timeout) has passed, emitting ${chunk.size} elements" }
                    this@channelFlow.send(chunk.toList())
                    chunk.clear()
                }
            }
        }
    }
}
