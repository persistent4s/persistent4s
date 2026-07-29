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

package persistent4s.postgres

import java.util.UUID

import scala.concurrent.duration.*

import cats.effect.*
import cats.effect.implicits.*
import cats.effect.std.SecureRandom
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

import persistent4s.LeaderElection

/** Lease-based leader election on top of a single Postgres row.
  *
  * ==Contract==
  *
  * At most one leader runs `task` at any time, '''provided every pause in this process is shorter than `ttl`'''. The
  * lease is guarded by a local monotonic deadline: renewals push the deadline forward, and if it is ever about to lapse
  * the task is cancelled. Every way of losing contact with the database — a lost lease, a hung connection, an exhausted
  * pool, a stop-the-world pause — funnels into the same outcome: the deadline stops advancing and `task` is cancelled
  * before the lease can expire elsewhere.
  *
  * This is '''not''' a distributed lock, for two reasons:
  *
  *   - Cancellation in cats-effect is cooperative. A `task` blocked inside a non-interruptible `blocking` call or an
  *     `uncancelable` region keeps running after the watchdog has given up. Brief overlap with a newly elected leader
  *     is therefore possible, and `task` must tolerate it.
  *   - There is no fencing token, so downstream systems cannot reject writes from a deposed leader.
  *
  * If the protected side effects genuinely must never overlap, they need to be idempotent or fenced at the resource
  * they write to.
  *
  * ==Failover==
  *
  * On graceful shutdown the lease is deleted, so a new leader is elected within roughly one `pollInterval`. On a hard
  * crash (SIGKILL, OOM, node loss) the row survives and the cluster stays leaderless for up to `ttl`. `ttl` is
  * therefore a direct trade between failover latency and tolerance for local pauses.
  *
  * `task` is started from scratch each time leadership is acquired; it is not resumed.
  */
final class PostgresLeaderElection[F[_]: Async: SecureRandom] private (
  pool: Resource[F, Session[F]],
  ttl: FiniteDuration,
  pollInterval: FiniteDuration,
  renewTimeout: FiniteDuration,
) extends LeaderElection[F]:

  import PostgresLeaderElection.*

  private val ttlMillis: Long = ttl.toMillis

  private val renewEvery: FiniteDuration = ttl / 3

  private val safetyMargin: FiniteDuration = (ttl / 10).max(500.millis)

  private val retryDelay: FiniteDuration = (renewEvery / 4).max(250.millis)

  private val watchdogEvery: FiniteDuration = (safetyMargin / 2).max(100.millis)

  private val pollJitter: FiniteDuration = pollInterval / 2

  override def runAsLeader(name: String)(task: F[Unit]): F[Unit] =

    /** Jittered, so standbys do not contend on the same row on the same cadence. */
    def sleepBeforeRetry: F[Unit] =
      SecureRandom[F]
        .betweenLong(0L, pollJitter.toMillis.max(1L))
        .flatMap(jitter => Async[F].sleep(pollInterval + jitter.millis))

    def acquireLoop: F[Unit] =
      UUIDGen.randomUUID[F].flatMap { holder =>
        Clock[F].monotonic.flatMap { sentAt =>
          tryAcquire(name, holder).timeout(renewTimeout).attempt.flatMap {
            case Right(true) => hold(name, holder, sentAt + ttl, task)
            case _           => sleepBeforeRetry *> acquireLoop
          }
        }
      }

    def hold(name: String, holder: UUID, deadlineAt: FiniteDuration, task: F[Unit]): F[Unit] =
      Ref.of[F, FiniteDuration](deadlineAt).flatMap { deadline =>
        renewLoop(name, holder, deadline).background
          .surround(Async[F].race(task, watchdog(deadline)))
          .guarantee(releaseQuietly(name, holder))
          .flatMap {
            case Left(())  => Async[F].unit // task finished on its own
            case Right(()) => acquireLoop   // deadline reached
          }
      }

    def renewLoop(name: String, holder: UUID, deadline: Ref[F, FiniteDuration]): F[Unit] =
      Async[F].sleep(renewEvery) *> renewOnce(name, holder, deadline)

    def renewOnce(name: String, holder: UUID, deadline: Ref[F, FiniteDuration]): F[Unit] =
      Clock[F].monotonic.flatMap { sentAt =>
        renew(name, holder).timeout(renewTimeout).attempt.flatMap {
          case Right(true) =>
            deadline.set(sentAt + ttl) *> renewLoop(name, holder, deadline)
          case Right(false) =>
            // The database answered: a standby owns the lease. Trip the watchdog now instead of waiting it out.
            deadline.set(Duration.Zero)
          case Left(_) =>
            // Timeout or transient error. We do not know whether we still hold it, so keep trying and let the
            // deadline decide.
            Async[F].sleep(retryDelay) *> renewOnce(name, holder, deadline)
        }
      }

    def watchdog(deadline: Ref[F, FiniteDuration]): F[Unit] =
      (Clock[F].monotonic, deadline.get).flatMapN { (now, deadlineAt) =>
        if now >= deadlineAt - safetyMargin then Async[F].unit
        else Async[F].sleep(watchdogEvery) *> watchdog(deadline)
      }

    acquireLoop

  private def tryAcquire(name: String, holder: UUID): F[Boolean] =
    pool.use(_.option(acquireQuery)(name, holder, ttlMillis)).map(_.contains(holder))

  private def renew(name: String, holder: UUID): F[Boolean] =
    pool.use(_.option(renewQuery)(ttlMillis, name, holder)).map(_.contains(holder))

  private def release(name: String, holder: UUID): F[Unit] =
    pool.use(_.execute(releaseCommand)(name, holder)).void

  /** Best-effort: a failed release only costs failover latency, never safety. */
  private def releaseQuietly(name: String, holder: UUID): F[Unit] =
    release(name, holder).timeout(renewTimeout).attempt.void

object PostgresLeaderElection:

  /** @param ttl
    *   how long a lease is valid. Bounds the leaderless window after a hard crash, and the local pause this process can
    *   survive without standing down.
    * @param pollInterval
    *   base delay between acquisition attempts by a standby; jittered by up to 50%.
    * @param renewTimeout
    *   how long a single acquire/renew/release query is waited on before being abandoned.
    */
  def make[F[_]: Async: SecureRandom](
    pool: Resource[F, Session[F]],
    ttl: FiniteDuration = 30.seconds,
    pollInterval: FiniteDuration = 5.seconds,
    renewTimeout: FiniteDuration = 5.seconds,
  ): PostgresLeaderElection[F] =
    require(ttl >= 1.second, s"ttl must be at least 1 second, was $ttl")
    require(pollInterval > Duration.Zero, s"pollInterval must be positive, was $pollInterval")
    require(renewTimeout > Duration.Zero, s"renewTimeout must be positive, was $renewTimeout")
    require(
      renewTimeout < ttl / 3,
      s"renewTimeout ($renewTimeout) must be shorter than the renewal interval (${ttl / 3}) so that a lease has room " +
        "for more than one attempt",
    )
    new PostgresLeaderElection(pool, ttl, pollInterval, renewTimeout)

  private[postgres] val createTableCommand: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS leader_leases (
        name       TEXT        PRIMARY KEY,
        holder     UUID        NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL
      )
    """.command

  def createTable[F[_]: Concurrent](pool: Resource[F, Session[F]]): F[Unit] =
    pool.use(_.execute(createTableCommand)).void

  // Insert the lease, or steal it only if the current one has already expired (DB clock). RETURNING yields a row iff
  // we now hold it.
  private val acquireQuery: Query[(String, UUID, Long), UUID] =
    sql"""
      INSERT INTO leader_leases (name, holder, expires_at)
      VALUES ($text, $uuid, clock_timestamp() + ($int8 * interval '1 millisecond'))
      ON CONFLICT (name) DO UPDATE
        SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
        WHERE leader_leases.expires_at < clock_timestamp()
      RETURNING holder
    """.query(uuid)

  // Push expiry forward only while we still hold it. RETURNING is empty if a standby has taken over.
  private val renewQuery: Query[(Long, String, UUID), UUID] =
    sql"""
      UPDATE leader_leases
      SET expires_at = clock_timestamp() + ($int8 * interval '1 millisecond')
      WHERE name = $text AND holder = $uuid
      RETURNING holder
    """.query(uuid)

  private val releaseCommand: Command[(String, UUID)] =
    sql"""
      DELETE FROM leader_leases WHERE name = $text AND holder = $uuid
    """.command
