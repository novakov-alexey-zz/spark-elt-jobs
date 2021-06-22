package etljobs.spark

object common {

  def useResource[T <: AutoCloseable](r: T)(f: T => Unit) =
    try f(r)
    finally r.close()
}
