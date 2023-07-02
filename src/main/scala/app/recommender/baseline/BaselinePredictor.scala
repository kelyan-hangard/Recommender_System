package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state: State = _

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // Compute the average rating per user
    val userAverages = ratingsRDD
      .map { case (userId, _, _, rating, _) => (userId, (rating, 1)) }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues { case (sum, count) => sum / count }

    // Compute the global average rating
    val globalAverage = userAverages.values.sum / userAverages.count

    // Compute the normalized deviations for each user and movie
    val normalizedDeviations = ratingsRDD
      .map { case (userId, movieId, _, rating, _) => (userId, (movieId, rating)) }
      .join(userAverages)
      .mapValues { case ((movieId, rating), avgRating) =>
        val deviation = rating - avgRating
        val scale = if (deviation > 0) 5 - avgRating else avgRating - 1
        (movieId, deviation / scale)
      }

    // Compute the average deviation per movie
    val movieDeviations = normalizedDeviations
      .map { case (userId, (movieId, deviation)) => (movieId, (deviation, 1)) }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues { case (sum, count) => sum / count }

    state = State(userAverages, globalAverage, movieDeviations)
  }

  def predict(userId: Int, movieId: Int): Double = {
    // Retrieve the user's average rating or use the global average if not available
    val userAverage = state.userAverages.lookup(userId).headOption.getOrElse(state.globalAverage)
    // Retrieve the movie's average deviation or use 0 if not available
    val movieDeviation = state.movieDeviations.lookup(movieId).headOption.getOrElse(0.0)

    // Calculate the scaling factor for the deviation
    val scale = if (movieDeviation > 0) 5 - userAverage else userAverage - 1
    // Compute the predicted rating using the user's average rating, movie's deviation, and scaling factor
    userAverage + movieDeviation * scale
  }

  case class State(
                    userAverages: RDD[(Int, Double)],
                    globalAverage: Double,
                    movieDeviations: RDD[(Int, Double)]
                  )
}
