-- Read model for the orders service.
--
-- `status` is the point of the whole example: OrderPlaced is provisional, so an order is Placed for as long as the saga
-- is in flight and only later becomes Confirmed or Cancelled. Any read model over a saga-driven aggregate needs a
-- column like this — there is no moment at which "the order exists" also means "the stock is reserved".
CREATE TABLE IF NOT EXISTS orders (
  order_id    UUID PRIMARY KEY,
  customer_id UUID NOT NULL,
  item_id     UUID NOT NULL,
  amount      INT  NOT NULL,
  status      TEXT NOT NULL,
  reason      TEXT
);
