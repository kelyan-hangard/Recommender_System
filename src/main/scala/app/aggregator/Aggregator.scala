package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {
  // Define the desired number of partitions for RDDs
  // We choose twice the number of cores as the number of partitions, which is a common practice for better performance.
  private val numCores = sc.defaultParallelism
  private val numPartitions = 2 * numCores

  // Set the partitioner using the calculated number of partitions
  private val partitioner: HashPartitioner = new HashPartitioner(numPartitions)

  private var state: RDD[(Int, (String, Double, List[String], Int))] = _

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {

    // Calculate the sum and count of ratings for each movieId
    val ratingSumsAndCounts = ratings
      .map { case (_, movieId, _, newRating, _) => (movieId, (newRating, 1)) }
      .reduceByKey(partitioner, (a, b) => (a._1 + b._1, a._2 + b._2))

    // Map movie titles with their genres
    val titleWithGenres = title.map { case (movieId, title, genres) => (movieId, (title, genres)) }

    // Create the state RDD with movie titles, average ratings, genres, and counts
    state = titleWithGenres
      .leftOuterJoin(ratingSumsAndCounts, partitioner)
      .mapValues {
        case ((title, genres), Some((sum, count))) => (title, sum / count, genres, count)
        case ((title, genres), None) => (title, 0.0, genres, 0)
      }
      .persist(MEMORY_AND_DISK)
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    // Map the state RDD to only include title and average rating
    state.map {
      case (_, (title, avgRating, _, _)) => (title, avgRating)
    }
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    // Filter titles based on the presence of all given keywords in the genres
    val filteredTitles = state.filter {
      case (_, (_, _, genres, _)) => keywords.forall(keyword => genres.contains(keyword))
    }

    // Filter rated titles
    val ratedTitles = filteredTitles.filter {
      case (_, (_, avgRating, _, _)) => avgRating > 0.0
    }

    // Calculate the average rating for the given keywords
    if (ratedTitles.isEmpty()) {
      if (filteredTitles.isEmpty()) -1.0 else 0.0
    } else {
      val (totalRating, count) = ratedTitles.map {
        case (_, (_, avgRating, _, _)) => (avgRating, 1)
      }.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      totalRating / count
    }
  }


  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */

  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // Parallelize the delta array
    val delta = sc.parallelize(delta_)

    // Calculate updated ratings and counts
    val updateRatings = delta.map {
      case (_, movieId, oldRating, newRating, _) =>
        oldRating match {
          case None => (movieId, (newRating, 1))
          case Some(oldRatingValue) =>
            val updatedRating = newRating - oldRatingValue
            (movieId, (updatedRating, 0))
        }
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    // Update the state RDD with new average ratings and counts
    val newState = state.leftOuterJoin(updateRatings).map {
      case (movieId, ((title, avgRating, genres, countOldRatings), Some((ratingChange, countNewRatings)))) =>
        val sumRatings = countOldRatings + countNewRatings
        val newAvgRating = (avgRating * countOldRatings + ratingChange) / sumRatings
        (movieId, (title, newAvgRating, genres, sumRatings))
      case (movieId, ((title, avgRating, genres, countOldRatings), None)) =>
        (movieId, (title, avgRating, genres, countOldRatings))
    }

    // Persist the new state RDD and unpersist the old state RDD
    newState.persist(MEMORY_AND_DISK)
    state.unpersist()
    state = newState
  }
}
