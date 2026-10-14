package kyo.compat

import cats.effect.IO
import cats.effect.kernel.Ref

/**
  * Uses `cats.effect.kernel.Ref[IO, Int]` through [[CAtomicRef]], which provides every operation.
  
  * See https://typelevel.org/cats-effect/api/3.x/cats/effect/kernel/Ref.html
 */
type CAtomicInt = CAtomicRef[Int]

object CAtomicInt {

  /**
    * Allocates a fresh atomic int initialized to `v`.
    */
  inline def init(inline v: Int): CIO[CAtomicInt] = CAtomicRef.init(v)

  /**
    * Wraps a native `cats.effect.kernel.Ref[IO, Int]` as a `CAtomicInt`. The conversion is the identity on the carrier.
    
    * See https://typelevel.org/cats-effect/api/3.x/cats/effect/kernel/Ref.html
 */
  inline def lift(inline u: Ref[IO, Int]): CAtomicInt = CAtomicRef.lift(u)
}
