package example.ws

import example.FINAM_SECRET_KEY
import grpc.tradeapi.v1.marketdata.SubscribeBarsResponse
import grpc.tradeapi.v1.marketdata.TimeFrame
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import ru.finam.tradeapi.*
import kotlin.time.Duration.Companion.seconds

object WsSubscribeBars {

    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        System.setProperty("logback.configurationFile", "/logback.xml")
        val secret = System.getenv(FINAM_SECRET_KEY)
        if (secret.isNullOrEmpty()) {
            throw IllegalStateException("$FINAM_SECRET_KEY env is not set")
        }
        val symbol = "SBERF@RTSX"
        val subscribeRequest = WsRequest.subscribeBarsRequest(symbol, TimeFrame.TIME_FRAME_D)
        val unsubscribeRequest = subscribeRequest.copy(action = Action.UNSUBSCRIBE)

        val client = WebsocketClient(secret = secret)
        val handshakeReceived = CompletableDeferred<Unit>()

        client.connect(handshakeReceived) { message ->
            val envelope = parseEnv(message) ?: throw RuntimeException("Failed to parse envelope $message")

            when (envelope.type) {
                MessageType.DATA -> {
                    if (envelope.subscriptionType == SubscriptionType.BARS) {
                        val barsResponse: SubscribeBarsResponse? = parseProto(
                            message,
                            { SubscribeBarsResponse.newBuilder() })
                        if (barsResponse == null) {
                            println("Failed to deserialize bars: ${envelope.payload}")
                        } else {
                            println("Received bars for symbol: ${barsResponse.symbol}")
                            barsResponse.barsList.forEach { bar ->
                                println(bar)
                            }
                        }
                    }
                }

                MessageType.EVENT -> println("Event received: ${envelope.eventInfo}")
                MessageType.ERROR -> println("Error received: ${envelope.errorInfo}")
            }

        }

        handshakeReceived.await()
        client.writeText(subscribeRequest)
        delay(30.seconds)
        client.writeText(unsubscribeRequest)
        client.close()
    }
}