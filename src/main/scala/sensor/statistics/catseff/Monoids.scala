package sensor.statistics.catseff

import cats._
import cats.implicits._

object Monoids {
  def monoid[A](zero: A)(plus: (A, A) => A): Monoid[A] = new Monoid[A] {
    @inline override def empty: A = zero

    @inline override def combine(x: A, y: A): A = plus(x, y)
  }

  case class Min[A](min: A) extends AnyVal

  case class Max[A](max: A) extends AnyVal

  implicit lazy val minMonoid: Monoid[Min[Int]] =
    monoid(Int.MaxValue)(Math.min).imap(Min(_))(_.min)

  implicit lazy val maxMonoid: Monoid[Max[Int]] =
    monoid(Int.MinValue)(Math.max).imap(Max(_))(_.max)
}
