package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime

import java.time.{Instant, ZoneId, ZonedDateTime}

class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null
  private var ratingsGroupedByYearByTitle: RDD[((Int, Int), Iterable[(Int, Option[Double], Double)])] = null
  private var titlesGroupedById: RDD[(Int, Iterable[(String, List[String])])] = null

  /**
   * Initialize the SimpleAnalytics class by creating partitioned and persisted RDDs
   * for the input ratings and movies data.
   *
   * @param ratings RDD of user ratings data
   * @param movie RDD of movie data
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {

    // Import SparkSession
    import org.apache.spark.sql.SparkSession

    // Create SparkSession with appName and local mode
    val spark: SparkSession = SparkSession.builder()
      .appName("YourAppName")
      .master("local[*]")
      .getOrCreate()

    // Access the SparkContext instance from SparkSession
    val sc = spark.sparkContext

    // Define the desired number of partitions for RDDs
    // We choose twice the number of cores as the number of partitions, which is a common practice for better performance.
    val numCores = sc.defaultParallelism
    val numPartitions = 2 * numCores

    // Create a HashPartitioner with the desired number of partitions
    val partitioner = new HashPartitioner(numPartitions)

    // Group movie data by movie ID and apply partitioning
    // We map the movie RDD to a tuple (movie_id, (title, genres)) and group by movie_id
    val titlesGroupedByIdd = movie
      .map {
        case (movie_id, title, genres) => (movie_id, (title, genres))
      }
      .groupByKey()
      .partitionBy(partitioner)

    // Process ratings data to group by year and movie ID, then apply partitioning
    // We map the ratings RDD to a tuple ((year, movieId), (userId, oldRating, newRating))
    val ratingsGroupedByYearAndMovieId = ratings
      .map {
        case (userId, movieId, oldRating, newRating, timestamp) =>
          val year = ZonedDateTime
            .ofInstant(Instant.ofEpochSecond(timestamp.toLong), ZoneId.systemDefault())
            .getYear
          ((year, movieId), (userId, oldRating, newRating))
      }
      .groupByKey()
      .partitionBy(partitioner)

    // Persist the processed RDDs for better performance
    // Using persist() allows Spark to keep the RDDs in memory for faster access in subsequent operations
    ratingsGroupedByYearByTitle = ratingsGroupedByYearAndMovieId.persist()
    titlesGroupedById = titlesGroupedByIdd.persist()
  }


  /**
   * Get the number of movies rated each year.
   *
   * @return An RDD containing tuples of (year, number of movies rated)
   */
  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    // Map each year to a count of 1
    val ratingsCountByYear = ratingsGroupedByYearByTitle
      .map { case ((year, _), _) => (year, 1) }
      // Sum the counts for each year
      .reduceByKey(_ + _)

    ratingsCountByYear
  }

  /**
   * Get the most rated movie for each year.
   *
   * @return An RDD containing tuples of (year, movie title)
   */
  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    // Map to tuples of (year, (movieId, number of ratings))
    val mostRatedMovieByYear = ratingsGroupedByYearByTitle
      .map { case ((year, movieId), ratings) => (year, (movieId, ratings.size)) }
      // Find the movie with the highest number of ratings or the highest movieId in case of ties
      .reduceByKey((a, b) => if (a._2 > b._2 || (a._2 == b._2 && a._1 > b._1)) a else b)
      // Map to tuples of (movieId, year)
      .map { case (year, (movieId, _)) => (movieId, year) }

    // Extract movie titles with their corresponding Ids
    val titlesWithId = titlesGroupedById.mapValues(_.head)

    // Join movie titles with the most rated movies by year
    val joinedRDD = mostRatedMovieByYear.join(titlesWithId)

    // Map to tuples of (year, movie title)
    val mostRatedMovieNameByYear = joinedRDD
      .map { case (_, (year, (title, _))) => (year, title) }

    mostRatedMovieNameByYear
  }

  /**
   * Get the most rated genre for each year.
   *
   * @return An RDD containing tuples of (year, list of genres)
   */
  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    // Map to tuples of (year, (movieId, number of ratings))
    val mostRatedMovieByYear = ratingsGroupedByYearByTitle
      .map { case ((year, movieId), ratings) => (year, (movieId, ratings.size)) }
      // Find the movie with the highest number of ratings or the highest movieId in case of ties
      .reduceByKey((a, b) => if (a._2 > b._2 || (a._2 == b._2 && a._1 > b._1)) a else b)
      // Map to tuples of (movieId, year)
      .map { case (year, (movieId, _)) => (movieId, year) }

    // Join the most rated movie with its genres by year
    val mostRatedMovieGenresByYear = mostRatedMovieByYear
      .join(titlesGroupedById.mapValues(_.head))
      // Map to tuples of (year, genres)
      .map { case (_, (year, (_, genres))) => (year, genres) }

    mostRatedMovieGenresByYear
  }

  /**
   * Get the most and least rated genres of all time.
   *
   * @return A tuple containing the least rated genre with its count and the most rated genre with its count
   */
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    val mostRatedGenresByYear = getMostRatedGenreEachYear

    val genreCounts = mostRatedGenresByYear
      .flatMap { case (_, genres) => genres.map(genre => (genre, 1)) }
      .reduceByKey(_ + _)
      .collect()

    implicit val GenreOrdering: Ordering[(String, Int)] = Ordering
      .by[(String, Int), (Int, String)](countAndGenre => (countAndGenre._2, countAndGenre._1.toLowerCase))

    val leastRatedGenre = genreCounts.minBy(genreCount => genreCount)(GenreOrdering)
    val mostRatedGenre = genreCounts.maxBy(genreCount => genreCount)(GenreOrdering)

    (leastRatedGenre, mostRatedGenre)
  }

  /**
   * Filter the movies RDD based on the required genres.
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    // Broadcast the requiredGenres RDD to all nodes
    val requiredGenresBroadcast: Broadcast[Set[String]] = movies.context.broadcast(requiredGenres.collect().toSet)

    // Filter the movies based on the required genres and extract movie names
    val filteredMovies = movies
      .filter { case (_, _, movieGenres) => movieGenres.exists(requiredGenresBroadcast.value.contains) }
      .map { case (_, movieName, _) => movieName }

    filteredMovies
  }

  /**
   * Filter the movies RDD based on the required genres using a broadcast callback.
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    // Use the broadcastCallback function to broadcast requiredGenres to all nodes
    val requiredGenresBroadcast: Broadcast[List[String]] = broadcastCallback(requiredGenres)

    // Filter the movies based on the required genres and extract movie names
    val filteredMovies = movies
      .filter { case (_, _, movieGenres) => movieGenres.exists(requiredGenresBroadcast.value.contains) }
      .map { case (_, movieName, _) => movieName }

    filteredMovies
  }
}
