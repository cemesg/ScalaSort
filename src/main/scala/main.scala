import scala.collection.immutable.{List, Nil}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Random, Success}


object main extends App {

  // Adjust MAX_DEPTH based on your system's capabilities and the size of the dataset
  val MAX_DEPTH = 4

  // Generate a list of random integers
  def generateRandomList(size: Int): List[Int] = {
    val rand = new Random
    List.fill(size)(rand.nextInt())
  }

  // Define a comparison function for integers
  def compareInts(x: Int, y: Int): Boolean = x < y

  @scala.annotation.tailrec
  def mergeTailRec[A](left: List[A], right: List[A], acc: List[A] = List.empty[A])(compare: (A, A) => Boolean): List[A] = (left, right) match {
    case (Nil, _) => acc.reverse ++ right
    case (_, Nil) => acc.reverse ++ left
    case (leftHead :: leftTail, rightHead :: rightTail) =>
      if (compare(leftHead, rightHead))
        mergeTailRec(leftTail, right, leftHead +: acc)(compare)
      else
        mergeTailRec(left, rightTail, rightHead +: acc)(compare)
  }

  def parallelMergeSort[A](list: List[A], depth: Int = 0)(compare: (A, A) => Boolean): Future[List[A]] = list match {
    case Nil => Future.successful(Nil)
    case _ :: Nil => Future.successful(list)
    case _ if depth < MAX_DEPTH =>
      val (left, right) = list.splitAt(list.length / 2)
      val leftSorted = parallelMergeSort(left, depth + 1)(compare)
      val rightSorted = parallelMergeSort(right, depth + 1)(compare)

      for {
        leftResult <- leftSorted
        rightResult <- rightSorted
      } yield mergeTailRec(leftResult, rightResult)(compare)
    case _ => Future.successful(sequentialMergeSortUsingTailRecMerge(list)(compare))
  }

  def sequentialMergeSortUsingTailRecMerge[A](list: List[A])(compare: (A, A) => Boolean): List[A] = list match {
    case Nil => Nil
    case _ :: Nil => list
    case _ =>
      val (left, right) = list.splitAt(list.length / 2)
      mergeTailRec(sequentialMergeSortUsingTailRecMerge(left)(compare), sequentialMergeSortUsingTailRecMerge(right)(compare))(compare)
  }


  // Test the sorting with a large list
  val listSize = 1000000
  val randomList = generateRandomList(listSize)

  val startTime = System.nanoTime()
  val sortedFuture = parallelMergeSort(randomList)(compareInts)
  Await.ready(sortedFuture, Duration.Inf)
  val endTime = System.nanoTime()

  val duration = (endTime - startTime) / 1e9d
  println(s"Sorting $listSize integers took $duration seconds.")

  sortedFuture.value match {
    case Some(Success(sortedList)) =>
      println(s"List is correctly sorted: ${sortedList == sortedList.sorted}")
    case Some(Failure(exception)) =>
      println(s"Sorting failed with an exception: $exception")
    case None =>
      println("Sorting did not complete.")
  }
}
