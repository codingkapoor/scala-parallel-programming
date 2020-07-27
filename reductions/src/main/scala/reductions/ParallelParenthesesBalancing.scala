package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    @tailrec
    def balanceR(index: Int, res: Int): Boolean = {
      if (index == chars.length) {
        if (res == 0) true
        else false
      } else {
        if (res < 0) false
        else {
          if (chars(index) == '(') balanceR(index + 1, res + 1)
          else if (chars(index) == ')') balanceR(index + 1, res - 1)
          else balanceR(index + 1, res)
        }
      }
    }

    balanceR(0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   *
   *                                             PASS
   *                                         "()(())" Nil
   *              "()(" List(1)                                       "())" List(-1)
   *   "(" List(1)             ")(" List(-1, 1)            "(" List(1)             "))" List(-1, -1)
   *               ")" List(-1)               "(" List(1)               ")" List(-1)               ")" List(-1)
   *
   *                                              FAIL
   *                                       ")((())" List(-1, 1)
   *             ")((" List(-1, 1, 1)                                 "())" List(-1)
   *   ")" List(-1)             "((" List(1, 1)            "(" List(1)             "))" List(-1, -1)
   *                  "(" List(1)             "(" List(1)               ")" List(-1)               ")" List(-1)
   *
   *
   *  Idea is to break down the given char array to it's last character, add +1 against '(' and -1 against ')' in a list
   *  and as we go up the complete array, we recursively merge these lists from left and right in such a way that only if
   *  left.last > 0 and right.head < 0 we remove those elements from individual lists and merge recursively.
   *
   *  merge(List(1, 1), List(1, 1)) -> List(1, 1, 1, 1)
   *  merge(List(1, 1, 1), List(-1, 1, 1)) -> List(1, 1, 1, 1)
   *  merge(List(1, 1, 1), List(-1, -1, 1)) -> List(1, 1)
   *
   *  The metric for a balanced string being that the list at the top of the tree should have reduced to Nil after all
   *  recursive merges.
   *
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    @tailrec
    def merge(ls1: List[Int], ls2: List[Int]): List[Int] = {
      if (ls1.isEmpty || ls2.isEmpty) ls1 ::: ls2
      else if (
        ls1.last < 0 && ls2.head > 0 ||
          ls1.last > 0 && ls2.head > 0 ||
          ls1.last < 0 && ls2.head < 0
      ) ls1 ::: ls2
      else { // ls1.last > 0 && ls2.head < 0
        if (ls1.size == 1) ls2
        else merge(ls1.take(ls1.size - 1), ls2.tail)
      }
    }

    def traverse(index: Int, until: Int): List[Int] = {
      if (until - index == 1) {
        if (chars(index) == '(') List(1)
        else if(chars(index) == ')') List(-1)
        else Nil
      } else {
        val mid = index + (until - index) / 2
        merge(traverse(index, mid), traverse(mid, until))
      }
    }

    def reduce(from: Int, until: Int): List[Int] = {
      if (until - from <= threshold) traverse(from, until)
      else {
        val mid = from + (until - from) / 2
        val (ls1, ls2) = parallel(reduce(from, mid), reduce(mid, until))
        ls1 ::: ls2
      }
    }

    reduce(0, chars.length).isEmpty
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
