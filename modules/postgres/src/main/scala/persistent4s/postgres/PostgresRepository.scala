package persistent4s.postgres

import persistent4s.Repository

//TODO: implement this repository to allow transactions when updating and deleting batches states and move persistStates logic into this interface
trait PostgresRepository[F[_], K, S] extends Repository[F, K, S] {}
