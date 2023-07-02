package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */

  import java.io.File
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    // Read the file and create an RDD of lines
    val lines = sc.textFile(new File(getClass.getResource(path).getFile).getPath)
    // Process each line and convert it into a tuple (userId, movieId, prevRating, newRating, timestamp)
    val ratingsRDD = lines.map(line => {
      // Split the line by "|" and extract the individual fields
      val tokens = line.split("\\|")
      val id_user = tokens(0).toInt
      val id_movie = tokens(1).toInt

      // Check if the line contains a previous rating
      if (tokens.length == 4) {
        val prev_rating = None
        val new_rating = tokens(2).toDouble
        val timestamp = tokens(3).toInt
        (id_user, id_movie, prev_rating, new_rating, timestamp)
      }
      else {
        val prev_rating = Some(tokens(2).toDouble)
        val new_rating = tokens(3).toDouble
        val timestamp = tokens(4).toInt
        (id_user, id_movie, prev_rating, new_rating, timestamp)
      }
    })

    // Persist the RDD
    ratingsRDD.persist()
    ratingsRDD
  }
}