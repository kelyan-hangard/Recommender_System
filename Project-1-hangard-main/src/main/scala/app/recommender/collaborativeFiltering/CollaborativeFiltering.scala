package app.recommender.collaborativeFiltering

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  private val maxIterations = 20
  private var model: MatrixFactorizationModel = _

  // Initialize the Collaborative Filtering model using the provided ratings data
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // Convert the input ratings data to the format required by ALS.train
    val alsRatings = ratingsRDD.map {
      case (userId, movieId, _, rating, _) => Rating(userId, movieId, rating)
    }

    // Train the ALS model using the specified parameters
    model = ALS.train(
      ratings = alsRatings,
      rank = rank,
      iterations = maxIterations,
      lambda = regularizationParameter,
      blocks = n_parallel,
      seed = seed
    )
  }

  // Predict the rating for a given user and movie using the trained ALS model
  def predict(userId: Int, movieId: Int): Double = {
    model.predict(userId, movieId)
  }
}
