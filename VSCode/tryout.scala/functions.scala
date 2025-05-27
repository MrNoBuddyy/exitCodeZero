


object functions {
  def multiply( x:Int,y:Int):Int={  ////a simple function which takes to integers as argument and returns an integer
    x*y
  }
  val divide:PartialFunction[(Int,Int),Int]={ ///a partial function which returns the division of two integers only when denominator is non-zero 
    case (x,y)if y!=0=>x/y 
 }

     val safeHead:PartialFunction[List[Int],Int]={  ///partial function which return the first element of the list
    case head ::_ =>head
}




///

    def main(args:Array[String]):Unit={
        println(s" multiplication of (6,2): $multiply(6, 2)")//simple function which return 12 in result
        
        try{
            val result =divide((6, 0))//when we try to call it with the input (6, 0), which is not defined for the partial function, it throws a MatchError
        }catch{
            //case Int => println(result);
            case e:MatchError =>println("MatchError: Division by zero is not allowed.")
        }
        println(s" safeHead(List(1, 2, 3)) : $safeHead(List(1, 2, 3))")  // 1
        println(s" safeHead Nil :$safeHead(Nil)")//will throw MatchError bcz the safeHead function is not defined for empty List
        try{
            val result=safeHead(List(5,4,3,6,7,9))
        }catch{
            case e:MatchError=>println("the function returned MatchError bcz the safeHead function is not defined for empty List ")
        }

        println(safeHead.isDefinedAt(List(0,1,2,3)))//
        //println(safeHead.isDefinedAt(List()))


    }

}
