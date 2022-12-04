package write_side

import akka.{NotUsed, actor}
import akka.actor.typed.ActorSystem
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.{Flow, Sink, Source}
import write_side.PersistenceActorExersixze.{Added, Multiplied}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

case class ReadSide(system: ActorSystem[NotUsed], persistenceId: PersistenceId) {

  implicit val materializer: actor.ActorSystem = system.classicSystem

  val repository = scala.collection.mutable.Seq.empty[OffsetInfo]

  val result: Future[(Int, Double)] = repository.find(_.id == 1) match {
    case Some(value) => Future.successful(value.offset, value.value)
    case None => Future.failed(new Exception("no values"))
  }

  var (offset, lastCalculation) = Await.result(result, 10.seconds)

  val startOffset: Int = if (offset == 1) 1 else offset + 1

  val readJournal: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  val source: Source[EventEnvelope, NotUsed] = readJournal
    .eventsByPersistenceId(persistenceId.id, startOffset, Long.MaxValue)

  val updateState: Flow[EventEnvelope, EventEnvelope, NotUsed] = Flow[EventEnvelope].map { event =>
    event.event match {
      case Added(_, amount) =>
        lastCalculation += amount
      case Multiplied(_, amount) =>
        lastCalculation *= amount
    }
    event
  }

  val updateRepoResult: Flow[EventEnvelope, Unit, NotUsed] = Flow[EventEnvelope].map { event =>
    repository.update(0, OffsetInfo(1, event.sequenceNr.toInt, lastCalculation))
  }

  source.async
    .via(updateState).async
    .via(updateRepoResult).async
    .runWith(Sink.ignore)
}

case class OffsetInfo(id: Int, offset: Int, value: Double)
