package example.ws

import example.FINAM_SECRET_KEY
import grpc.tradeapi.v1.marketdata.SubscribeBarsResponse
import grpc.tradeapi.v1.marketdata.TimeFrame
import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.finam.tradeapi.*
import kotlin.time.Duration.Companion.seconds


object WsSubscribeBars {

    private val logger: Logger = LoggerFactory.getLogger(WsSubscribeBars::class.java)

    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        val secret = System.getenv(FINAM_SECRET_KEY)
        if (secret.isNullOrEmpty()) {
            throw IllegalStateException("$FINAM_SECRET_KEY env is not set")
        }
        val symbol = "SBERF@RTSX"
        val subscribeRequest = WsRequest.subscribeBarsRequest(symbol, TimeFrame.TIME_FRAME_D)
        val unsubscribeRequest = subscribeRequest.copy(action = Action.UNSUBSCRIBE)

        val client = WebsocketClient()
        val handshakeReceived = CompletableDeferred<Unit>()

        val session = client.connect(secret = secret)

        val job = CoroutineScope(Dispatchers.IO).launch {
            delay(500)
            session.incoming.collect { message ->
                if (isHandshake(message)) {
                    handshakeReceived.complete(Unit)
                }
                val envelope = parseEnv(message) ?: throw RuntimeException("Failed to parse envelope $message")


                when (envelope.type) {
                    MessageType.DATA -> {
                        if (envelope.subscriptionType == SubscriptionType.BARS) {
                            val barsResponse: SubscribeBarsResponse? = parseProto(
                                message,
                                { SubscribeBarsResponse.newBuilder() })
                            if (barsResponse == null) {
                                logger.error("Failed to deserialize bars: ${envelope.payload}")
                            } else {
                                logger.info("Received bars: \n {}", barsResponse)
                            }
                        }
                    }

                    MessageType.EVENT -> logger.info("Event received: ${envelope.eventInfo}")
                    MessageType.ERROR -> logger.error("Error received: ${envelope.errorInfo}")
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