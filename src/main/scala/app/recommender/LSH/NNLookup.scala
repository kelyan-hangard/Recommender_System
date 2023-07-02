package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    // Hash the queries using the LSH index
    val hashedQueries = lshIndex.hash(queries)
    // Perform the lookup on the hashed queries and map the results
    lshIndex.lookup(hashedQueries).map { case (_, keywords, results) => (keywords, results) }
  }
}
