package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  import java.io.File

  def load(): RDD[(Int, String, List[String])] = {
    // Read the file and create an RDD of lines
    val lines  = sc.textFile(new File(getClass.getResource(path).getFile).getPath)
    // Process each line and convert it into a tuple (movieId, movieName, genres)
    val moviesRDD = lines.map(line => {
      // Split the line by "|" and extract the individual fields
      val tokens  = line.split("\\|")
      val id = tokens (0).toInt
      val name = tokens (1).replaceAll("\"", "")
      val genres = tokens.drop(2).map(_.replaceAll("\"", "")).toList
      (id, name, genres)
    })

    // Persist the RDD
    moviesRDD.persist()

    moviesRDD
  }
}