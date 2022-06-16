package io.provenance.coroutines.channels

import io.kotest.core.spec.style.AnnotationSpec
import io.kotest.matchers.collections.shouldBeSorted
import io.kotest.matchers.collections.shouldContainAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.toList

private suspend operator fun <T> Channel<T>.plusAssign(item: T) { send(item) }
private suspend operator fun <T> Channel<T>.plusAssign(items: Iterable<T>) { items.forEach { send(it) } }

private suspend operator fun <T> Channel<T>.plus(item: T) = (toList() + item).asChannel()
private suspend operator fun <T> Channel<T>.plus(items: Iterable<T>) = (toList() + items).asChannel()

private suspend fun <T> List<T>.asChannel(): Channel<T> = Channel<T>(size).apply { forEach { send(it) } }.apply { close() }

class ChannelExtensionsTest : AnnotationSpec() {
    init {
        coroutineTestScope = true
    }

    @Test
    suspend fun testReceiveChannelReceiveQueued() {
        val list = listOf<Int>() + 1 + 2 + 3
        val chan = list.asChannel()

        val recv = chan.receiveQueued()
        val recvList = recv.toList()
        recv.shouldContainAll(list)
        recv.shouldBeSorted()
    }
}
