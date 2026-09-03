/*
 * Copyright 2026 Antonio Jimenez and Bastien Jolidon
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package persistent4s.examples.saga.orders.saga

import persistent4s.examples.saga.contract.{AuthorizePayment, CancelPayment, ReleaseStock, ReserveStock}

/** Everything this saga can ask for, so its signatures say so and nothing else can be sent by accident.
  *
  * A bare type alias now. It used to need a companion carrying a hand-written `MessageEncoder` that matched on all four
  * leaves — each case pairing a type with the codec and the kind string that went with it, three agreements kept by
  * eye. `SagaRequest` captures the encoder and the name from the payload's own declaration at construction, so the
  * match had nothing left to decide.
  */
type OrderRequest = ReserveStock | ReleaseStock | AuthorizePayment | CancelPayment
