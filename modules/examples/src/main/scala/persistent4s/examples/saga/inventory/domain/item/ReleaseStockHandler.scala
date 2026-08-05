package persistent4s.examples.saga.inventory.domain.item

import persistent4s.examples.saga.contract.ReleaseStock
import persistent4s.CommandHandler
import persistent4s.Tag
import persistent4s.examples.saga.inventory.domain.{InventoryEvent, InventoryTags, StockReserved, StockReleased}

final case class ReleaseState(amount: Option[Int])

object ReleaseStockHandler extends CommandHandler[ReleaseStock, ReleaseState, InventoryEvent]:

  override def tags(command: ReleaseStock): Set[Tag] = Set(InventoryTags.item(command.itemId))

  override def evolve(command: ReleaseStock, state: ReleaseState, event: InventoryEvent): ReleaseState =
    event match
      case StockReserved(_, orderId, amount) if orderId == command.orderId => ReleaseState(Some(amount))
      case StockReleased(_, orderId, amount) if orderId == command.orderId => ReleaseState(None)
      case _                                                               => state

  def initial: ReleaseState = ReleaseState(None)

  def validate(state: ReleaseState, command: ReleaseStock): Either[Throwable, Unit] = Right(())

  def decide(state: ReleaseState, command: ReleaseStock): List[(Set[Tag], InventoryEvent)] =
    state.amount match
      case Some(amt) =>
        List(Set(InventoryTags.item(command.itemId)) -> StockReleased(command.itemId, command.orderId, amt))
      case None => Nil
