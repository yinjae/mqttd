package plantae.citrus.mqtt.actors.session

import akka.actor.{ActorLogging, ActorRef, FSM}
import plantae.citrus.mqtt.actors.SystemRoot
import plantae.citrus.mqtt.packet.{FixedHeader, PublishPacket}
import scodec.bits.ByteVector


case object InvokePublish

case class StorageRequest(publishPacket: PublishPacket)

sealed trait SessionStorageState


case object NoneMessage extends SessionStorageState

case object HasRedoMessage extends SessionStorageState

case object HasReadyMessage extends SessionStorageState

sealed trait SessionStorageContainer

case class MessageContainer(currentPacketId: Int, readyQueue: List[QueueMessage], redoQueue: List[PublishPacket], workQueue: List[PublishPacket]) extends SessionStorageContainer

case class Enqueue(payload: Array[Byte], qos: Short, retain: Boolean, topic: String)

case class QueueMessage(payload: Array[Byte], qos: Short, retain: Boolean, topic: String)

case class EvictQueue(packetId: Int)

case object ConnectionClosed

class Storage(sessionActor: ActorRef) extends FSM[SessionStorageState, SessionStorageContainer] with ActorLogging {
  private val chunkSize = {
    try {
      if (SystemRoot.config.hasPath("mqtt.broker.session.chunk.size"))
        SystemRoot.config.getInt("mqtt.broker.session.chunk.size")
      else 20
    } catch {
      case t: Throwable => 20
    }
  }
  startWith(NoneMessage, MessageContainer(1, Nil, Nil, Nil))

  when(NoneMessage) {
    case Event(enqueue: Enqueue, container: MessageContainer) =>
      self ! InvokePublish
      goto(HasReadyMessage) using MessageContainer(container.currentPacketId, container.readyQueue :+ QueueMessage(enqueue.payload, enqueue.qos, enqueue.retain, enqueue.topic), container.redoQueue, container.workQueue)

    case Event(InvokePublish, container: MessageContainer) =>
      log.info("InvokePublish:NoneMessage")
      stay() using container

    case Event(complete: EvictQueue, container: MessageContainer) =>
      stay using MessageContainer(container.currentPacketId, container.readyQueue, container.redoQueue, container.workQueue.filter(_.packetId match {
        case Some(x) => x != complete.packetId
        case None => true
      }))

    case Event(ConnectionClosed, container: MessageContainer) =>
      container.redoQueue ++ container.workQueue match {
        case Nil => stay using MessageContainer(container.currentPacketId, container.readyQueue, Nil, Nil)
        case redoQueue: List[PublishPacket] => goto(HasRedoMessage) using MessageContainer(container.currentPacketId, container.readyQueue, redoQueue, Nil)
      }

  }

  def nextPacketId(currentPacketId: Int): Int = {
    if (currentPacketId + 1 >= Short.MaxValue) 1
    else currentPacketId + 1
  }

  when(HasReadyMessage) {
    case Event(enqueue: Enqueue, container: MessageContainer) =>

      stay using MessageContainer(container.currentPacketId, container.readyQueue :+ QueueMessage(enqueue.payload, enqueue.qos, enqueue.retain, enqueue.topic), container.redoQueue, container.workQueue)

    case Event(InvokePublish, container: MessageContainer) =>
      if (container.workQueue.size >= chunkSize) {
        //        log.info("InvokePublish:HasReadyMessage {} {}", container.workQueue.size, container.readyQueue.size)
        stay using container
      } else {
        val message = container.readyQueue.head
        val publishPacket = PublishPacket(FixedHeader(dup = false, qos = message.qos, retain = message.retain),
          topic = message.topic,
          packetId = {
            if (message.qos == 0) None else Some(container.currentPacketId)
          },
          ByteVector(message.payload)
        )

        sessionActor ! StorageRequest(publishPacket)

        def workQueue = publishPacket.fixedHeader.qos match {
          case 0 => container.workQueue
          case x if (x > 0) => container.workQueue :+ publishPacket
        }
        container.readyQueue.tail match {
          case Nil => goto(NoneMessage) using MessageContainer(nextPacketId(container.currentPacketId), Nil, container.redoQueue, workQueue)
          case tail => stay using MessageContainer(nextPacketId(container.currentPacketId), tail, container.redoQueue, workQueue)
        }
      }

    case Event(complete: EvictQueue, container: MessageContainer) =>
      stay using MessageContainer(container.currentPacketId, container.readyQueue, container.redoQueue, container.workQueue.filter(_.packetId match {
        case Some(x) => x != complete.packetId
        case None => true
      }))

    case Event(ConnectionClosed, container: MessageContainer) =>
      container.redoQueue ++ container.workQueue match {
        case Nil => stay using MessageContainer(container.currentPacketId, container.readyQueue, Nil, Nil)
        case redoQueue: List[PublishPacket] => goto(HasRedoMessage) using MessageContainer(container.currentPacketId, container.readyQueue, redoQueue, Nil)
      }
  }


  when(HasRedoMessage) {
    case Event(enqueue: Enqueue, container: MessageContainer) =>
      stay using MessageContainer(container.currentPacketId, container.readyQueue :+ QueueMessage(enqueue.payload, enqueue.qos, enqueue.retain, enqueue.topic), container.redoQueue, container.workQueue)

    case Event(InvokePublish, container: MessageContainer) =>
      if (container.workQueue.size >= chunkSize) {
        stay using container
      } else {
        val publishPacket = container.redoQueue.head

        sessionActor ! StorageRequest(publishPacket)

        def workQueue = publishPacket.fixedHeader.qos match {
          case 0 => container.workQueue
          case x if (x > 0) => container.workQueue :+ publishPacket
        }
        container.redoQueue.tail match {
          case Nil =>

            container.readyQueue match {
              case Nil => goto(NoneMessage) using MessageContainer(container.currentPacketId, Nil, Nil, workQueue)
              case readyQueue => goto(HasReadyMessage) using MessageContainer(container.currentPacketId, readyQueue, Nil, workQueue)
            }
          case tail: List[PublishPacket] => stay using MessageContainer(container.currentPacketId, container.readyQueue, tail, workQueue)
        }

      }

    case Event(complete: EvictQueue, container: MessageContainer) =>
      stay using MessageContainer(container.currentPacketId, container.readyQueue, container.redoQueue, container.workQueue.filter(_.packetId match {
        case Some(x) => x != complete.packetId
        case None => true
      }))

    case Event(ConnectionClosed, container: MessageContainer) =>
      stay using MessageContainer(container.currentPacketId, container.readyQueue, container.redoQueue ++ container.workQueue, Nil)
  }


  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
  }

  initialize()
}
