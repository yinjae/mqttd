package plantae.citrus.mqtt.actors.pahotest

import org.eclipse.paho.client.mqttv3._

object TopicTest extends App {
  var option = new MqttConnectOptions()
  var client1 = new MqttClient("tcp://127.0.0.1:1883", "customer_1")
  client1.setCallback(
    new MqttCallback {
      override def deliveryComplete(iMqttDeliveryToken: IMqttDeliveryToken): Unit = {}

      override def messageArrived(s: String, mqttMessage: MqttMessage): Unit = {
        println("client1 1 => topic:" + s + "\tmessage:" + new String(mqttMessage.getPayload))
      }

      override def connectionLost(throwable: Throwable): Unit = {}
    }
  )
  option.setKeepAliveInterval(10)
  client1.connect(option)

  var client2 = new MqttClient("tcp://127.0.0.1:1883", "customer_2")
  client2.setCallback(
    new MqttCallback {
      override def deliveryComplete(iMqttDeliveryToken: IMqttDeliveryToken): Unit = {}

      override def messageArrived(s: String, mqttMessage: MqttMessage): Unit = {
        println("client2 1 => topic:" + s + "\tmessage:" + new String(mqttMessage.getPayload))
      }

      override def connectionLost(throwable: Throwable): Unit = {}
    }
  )
  option.setKeepAliveInterval(10)
  client2.connect(option)

  ////////////////
  client1.subscribe("a/#", 2)
  client1.subscribe("a/+", 2)
  client1.subscribe("a/b", 1)
  client2.publish("a/b", "a/b topic".getBytes, 0, false)







}
