import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.hasElement
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionInitialPosition
import org.apache.pulsar.client.api.SubscriptionType
import org.apache.pulsar.common.policies.data.Policies
import org.apache.pulsar.common.policies.data.TenantInfoImpl
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertNotNull

class CantUseTopicAfterGCTest {

    private val pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build()
    private val adminClient = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").build()

    @BeforeEach
    fun setUp() {
        val clusters = adminClient.clusters().clusters.toSet()
        try {
            adminClient.tenants().createTenant("localdev", TenantInfoImpl(emptySet(), clusters))
        } catch (e: PulsarAdminException.ConflictException) {
            //tenant already exists
        }
        try {
            adminClient.namespaces().createNamespace("localdev/test", Policies())
        } catch (e: PulsarAdminException.ConflictException) {
            //namespace already exists
        }
    }

    @Test
    fun `should be able to consume messages after topic is GC'd`() {
        val topic = "persistent://localdev/test/${UUID.randomUUID()}"

        assertTopicNotThere(topic)

        sendSomeMessages(topic)

        assertTopicIsThere(topic)

        waitForGC(topic)

        sendAndConsumeSomeMessages(topic)
    }

    private fun waitForGC(topic: String) {
        println("Waiting for GC")
        await().atMost(5, TimeUnit.MINUTES).untilAsserted {
            assertTopicNotThere(topic)
        }
        println("GC'd")
    }

    private fun sendAndConsumeSomeMessages(topic: String) {
        pulsarClient
            .newConsumer(Schema.STRING)
            .topic(topic)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscriptionName("test-sub")
            .subscribe().use { consumer ->
                sendSomeMessages(topic)
                val message = consumer.receive(10, TimeUnit.SECONDS)
                assertNotNull(message, "never received a message")
            }
    }

    private fun sendSomeMessages(topic: String) {
        pulsarClient
            .newProducer(Schema.STRING)
            .topic(topic)
            .create().use { producer ->
                repeat(5) {
                    val message = UUID.randomUUID().toString()
                    println("Sending message: $message")
                    producer.newMessage().value(message).send()
                }
            }
    }

    private fun assertTopicNotThere(topic: String) {
        assertThat(adminClient.topics().getList("localdev/test"), !hasElement(topic))
    }

    private fun assertTopicIsThere(topic: String) {
        assertThat(adminClient.topics().getList("localdev/test"), hasElement(topic))
    }
}