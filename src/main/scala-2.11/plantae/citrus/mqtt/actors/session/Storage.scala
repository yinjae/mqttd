package plantae.citrus.mqtt.actors.session

import akka.actor.{ActorLogging, ActorRef, FSM, Terminated}
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

case class Enqueue(payload: ByteVector, qos: Short, retain: Boolean, topic: String)

case class QueueMessage(payload: ByteVector, qos: Short, retain: Boolean, topic: String)

case class EvictQueue(packetId: Int)

case object ConnectionClosed

class Storage(session: ActorRef) extends FSM[SessionStorageState, SessionStorageContainer] with ActorLogging {
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
    case Event(terminated: Terminated, container: MessageContainer) =>
      if (terminated.getActor == session) stop(FSM.Shutdown)
      else stay using container
  }


  when(HasReadyMessage) {
    case Event(enqueue: Enqueue, container: MessageContainer) =>

      stay using MessageContainer(container.currentPacketId, container.readyQueue :+ QueueMessage(enqueue.payload, enqueue.qos, enqueue.retain, enqueue.topic), container.redoQueue, container.workQueue)

    case Event(InvokePublish, container: MessageContainer) =>
      if (container.workQueue.size >= chunkSize) {
        stay using container
      } else {
        val message = container.readyQueue.head
        val publishPacket = PublishPacket(FixedHeader(dup = false, qos = message.qos, retain = message.retain),
          topic = message.topic,
          packetId = {
            if (message.qos == 0) None else Some(container.currentPacketId)
          },
          message.payload
        )

        session ! StorageRequest(publishPacket)

        def workQueue = publishPacket.fixedHeader.qos match {
          case 0 => container.workQueue
          case x if (x > 0) => container.workQueue :+ publishPacket
        }

        def nextPacketId: Int = {
          if (container.currentPacketId + 1 >= Short.MaxValue) 1
          else container.currentPacketId + 1
        }

        container.readyQueue.tail match {
          case Nil => goto(NoneMessage) using MessageContainer(nextPacketId, Nil, container.redoQueue, workQueue)
          case tail => stay using MessageContainer(nextPacketId, tail, container.redoQueue, workQueue)
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

    case Event(terminated: Terminated, container: MessageContainer) =>
      if (terminated.getActor == session) stop(FSM.Shutdown)
      else stay using container

  }


  when(HasRedoMessage) {
    case Event(enqueue: Enqueue, container: MessageContainer) =>
      stay using MessageContainer(container.currentPacketId, container.readyQueue :+ QueueMessage(enqueue.payload, enqueue.qos, enqueue.retain, enqueue.topic), container.redoQueue, container.workQueue)

    case Event(InvokePublish, container: MessageContainer) =>
      if (container.workQueue.size >= chunkSize) {
        stay using container
      } else {
        val publishPacket = container.redoQueue.head

        session ! StorageRequest(publishPacket)

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

    case Event(terminated: Terminated, container: MessageContainer) =>
      if (terminated.getActor == session) stop(FSM.Shutdown)
      else stay using container
  }


  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    context.watch(session)
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
  }

  initialize()
}
