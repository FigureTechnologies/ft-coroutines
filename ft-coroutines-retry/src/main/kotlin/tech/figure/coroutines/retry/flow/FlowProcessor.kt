package tech.figure.coroutines.retry.flow

/**
 * Callback hook for submitting an item into a retry flow.
 */
interface FlowProcessor<T> {
    /**
     * Send an item into this retry flow for later processing
     *
     * @param item The item to send into the hopper.
     */
    suspend fun send(item: T, e: Throwable)

    /**
     * Process an item.
     *
     * @param item The item that is being processed.
     */
    suspend fun process(item: T, attempt: Int = 0)
}
