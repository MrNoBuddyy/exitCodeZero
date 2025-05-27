import java.io._
import scala.io._
 

object indexOfNumInList {
    def main(arg:Array[String]):Unit={
        var list:List[Int]=List(1,2,3,4,5,6,7,8,9,12,89,56,73,91);
        var q:Int=scala.io.StdIn.readInt();
        var ind=list.indexOf(q);
        println(ind);
    
}
}
