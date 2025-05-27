
import java.io._
import scala.io._
      
object factorial {
/// factorial function using recursive function
def fact(n:Int):Int={
    if(n==1)return 1;
    return n*fact(n-1);

}
// factorial using loops
def factLoop(n:Int):Int={
    var fac:Int=1;
    for(i <-1 to n){
            fac*=i;
        }
        fac;
}
  def main(args:Array[String])={
        var n:Int=StdIn.readInt();
        println(fact(n));
        println(factLoop(n));
       
    }
}



object tryOne{
    
}
