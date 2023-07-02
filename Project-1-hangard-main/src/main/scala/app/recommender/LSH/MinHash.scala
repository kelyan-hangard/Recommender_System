package app.recommender.LSH

/**
 * Helper class for computing LSH signatures
 *
 * @param seed Seeds for "randomizing" the produced signatures
 */
class MinHash(seed : IndexedSeq[Int]) extends Serializable {
  /**
   * Hash function for a single keyword
   *
   * @param key  The keyword that is hashed
   * @param seed Seeds for "randomizing" the produced signatures
   * @return The signature for the given keyword
   */
  def hashSeed(key: String, seed: Int): Int = {
    val k = (key + seed.toString).hashCode
    val k1 = k * 0xcc9e2d51
    val k2 = k1 >> 15
    val k3 = k2 * 0x1b873593
    val k4 = k3 >> 13
    k4.abs
  }

  /**
   * Hash function for a list of keywords. The keywords are combined using
   * the MinHash approach.
   *
   * @param data The list of keywords that is hashed
   * @return The signature for the given list of keyword (and seeds)
   */
  def hash(data: List[String]) : IndexedSeq[Int] = {
    seed.map(s => data.map(y => hashSeed(y, s)).min)
  }
}
