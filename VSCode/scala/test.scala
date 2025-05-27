import scala.util.matching.Regex

object test {
  def main(args: Array[String]): Unit = {
    // Applying Regex class
    val Geeks = new Regex("(G|g)(f|F)G")
    val y = "GfG is a CS portal. I like gFG"
    // println((Geeks findAllIn y).mkString(", "))
    val a = Geeks.findAllIn(y)
    a.foreach(println)

    val regEx = "[a-zA-Z0-9]+[\\.\\-\\_]*[@]+[a-z][\\.]+[a-z]".r()
    val email = "vijay185.kumar@ril.com"
    val result = regEx.findFirstIn(email)
    println(regEx findAllIn email)
    result.foreach(println)
  }
}
