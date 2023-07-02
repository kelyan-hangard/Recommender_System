package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    // Get the set of movie IDs watched by the user
    val userWatchedMovies = ratings.filter(_._1 == userId).map(_._2).collect().toSet

    // Find similar movies for the given genre, excluding movies the user has already watched
    val genreSimilarMovies = nn_lookup.lookup(sc.parallelize(List(genre))).collect().head._2.filterNot(movie => userWatchedMovies.contains(movie._1))

    // Compute predicted ratings using the baseline predictor for the remaining similar movies
    val baselineRecommendations = genreSimilarMovies
      .map(movie => (movie._1, baselinePredictor.predict(userId, movie._1))) // (movieId, predictedRating)
      .sortBy(-_._2) // Sort by descending predicted rating
      .take(K) // Take the top K movies

    baselineRecommendations
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    // Get the set of movie IDs watched by the user
    val userWatchedMovies = ratings.filter(_._1 == userId).map(_._2).collect().toSet

    // Find similar movies for the given genre, excluding movies the user has already watched
    val genreSimilarMovies = nn_lookup.lookup(sc.parallelize(List(genre))).collect().head._2.filterNot(movie => userWatchedMovies.contains(movie._1))

    // Compute predicted ratings using the collaborative filtering predictor for the remaining similar movies
    val collaborativeRecommendations = genreSimilarMovies
      .map(movie => (movie._1, collaborativePredictor.predict(userId, movie._1))) // (movieId, predictedRating)
      .sortBy(-_._2) // Sort by descending predicted rating
      .take(K) // Take the top K movies

    collaborativeRecommendations
  }
}
