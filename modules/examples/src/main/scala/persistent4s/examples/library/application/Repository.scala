package persistent4s.examples.library.application

trait Repository[F[_], K, V]:

  def find(key: K): F[Option[V]]

  def save(key: K, value: V): F[Unit]

  def delete(key: K): F[Unit]
