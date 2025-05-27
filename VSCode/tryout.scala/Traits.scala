//trait encapsulates method and field definitions that can be used in classes
//distinguishing quality or characteristic

trait Equal {
  def isEqual(x: Any): Boolean

  def isNotEqual(x: Any): Boolean = {
    !isEqual(x)
  }
}
  class Point(xc:Int,yc:Int)extends Equal{
    var x:Int=xc
    var y:Int=yc
    def isEqual(obj:Any)=obj.isInstanceOf[Point]&&
      obj.asInstanceOf[Point].x==x

  }
object Traits{
  def main(args: Array[String]): Unit = {
    val p1=new Point(2,4)
    val p2=new Point(4,6)
    val p3=new Point(6,6)
    println(p1.isNotEqual(p2))
    println(p1.isNotEqual(p3))
    println(p2.isNotEqual(2))
  }
}