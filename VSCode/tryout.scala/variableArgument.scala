package variableArgument
//import scala._

object Main {
  def sum ( num:Int *):Int={
    var ans=0
    for(i<-num){
      ans+=i
    }
    return ans
  }
  def main(args: Array[String]): Unit = {
//    var s:String=scala.io.StdIn.readLine()
//    for(i<-s){
//      if(i!=' ')print(s"$i ")
//    }

    println(sum(2,3,4,5,6))
    println(sum(1 ,2))
    println(sum(3,5,98,21))
  }
}
