package write_side

import akka.persistence.typed.PersistenceId
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl._
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl._


object PersistenceActorExersixze extends App {

  sealed trait Command

  case class Add(amount: Int) extends Command

  case class Multiply(amount: Int) extends Command

  sealed trait Event

  case class Added(id: Int, amount: Int) extends Event

  case class Multiplied(id: Int, multiplier: Int) extends Event

  final case class State(value: Int) {
    def add(amount: Int): State = copy(value = value + amount)

    def multiply(amount: Int): State = copy(value = value * amount)
  }

  object State {
    val empty = State(0)
  }

  def process(persistenceId: PersistenceId): Behavior[Command] =
    Behaviors.setup { ctx =>
      EventSourcedBehavior[Command, Event, State](
        persistenceId = persistenceId,
        State.empty,
        (_, command) => handleCommand(persistenceId.id, command),
        (state, event) => handleEvent(state, event)
      )
        .snapshotWhen {
          case (_, _, seqNumber) if seqNumber % 10 == 0 => true
          case _ => false
        }
        .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100, keepNSnapshots = 2))
    }

  def handleCommand(
                     persistenceId: String,
                     command: Command,
                   ): Effect[Event, State] =
    command match {
      case Add(amount) =>
        val added = Added(persistenceId.toInt, amount)
        Effect.persist(added)
      case Multiply(amount) => Effect.persist(Multiplied(persistenceId.toInt, amount))
    }

  def handleEvent(state: State, event: Event): State =
    event match {
      case Added(_, amount) => state.add(amount)
      case Multiplied(_, amount) => state.multiply(amount)
    }

}
