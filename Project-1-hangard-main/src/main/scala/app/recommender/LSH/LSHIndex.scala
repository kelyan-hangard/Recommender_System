package app.recommender.LSH


import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {

  // Initialize the MinHash instance with the provided seed
  private val minhash = new MinHash(seed)

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */

  def getBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    // Compute the MinHash signature for each title's keyword list
    val hashedData = data.map { case (id, title, keywords) => (minhash.hash(keywords), (id, title, keywords)) }
    // Group the data by the MinHash signature
    val groupedData = hashedData.groupByKey()
    // Convert the grouped data to a list for each signature
    groupedData.mapValues(_.toList)
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {
    // Convert the grouped data to a list for each signature
    val buckets = getBuckets()
    // Perform the lookup by joining the queries with the buckets based on the MinHash signature
    queries.join(buckets).map { case (signature, (payload, results)) => (signature, payload, results) }
  }
}
