package example.ws

import example.FINAM_SECRET_KEY
import kotlinx.coroutines.*
import ru.finam.tradeapi.Action
import ru.finam.tradeapi.WebsocketClient
import ru.finam.tradeapi.WsRequest
import ru.finam.tradeapi.isHandshake
import kotlin.time.Duration.Companion.seconds


abstract class WsSubscriptionBaseExample {

    fun run(subscribeRequest: WsRequest, onMessage: (String) -> Unit) = runBlocking {
        val secret = System.getenv(FINAM_SECRET_KEY)
        if (secret.isNullOrEmpty()) {
            throw IllegalStateException("$FINAM_SECRET_KEY env is not set")
        }
        val unsubscribeRequest = subscribeRequest.copy(action = Action.UNSUBSCRIBE)

        val client = WebsocketClient()
        val handshakeReceived = CompletableDeferred<Unit>()

        val session = client.connect(host = "cbdev.finam.ru", secret = secret)

        val job = CoroutineScope(Dispatchers.IO).launch {
            delay(500)
            session.incoming.collect { message ->
                if (isHandshake(message)) {
                    handshakeReceived.complete(Unit)
                } else {
                    onMessage(message)
                }
            }
        }

        handshakeReceived.await()
        session.send(subscribeRequest)
        delay(30.seconds)
        session.send(unsubscribeRequest)
        session.close.invoke()
        job.cancelAndJoin()
    }

}