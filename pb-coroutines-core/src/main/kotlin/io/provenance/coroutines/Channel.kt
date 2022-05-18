package io.provenance.coroutines

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel

@OptIn(ExperimentalCoroutinesApi::class)
suspend fun <T> ReceiveChannel<T>.receiveAllPending(): List<T> {
    val list = mutableListOf<T>()

    var item: T? = null
    do {
        item = receiveCatching().getOrNull()
        if (item != null) {
            list.add(item)
        }
    } while (item != null)
    return list
}