import java.io._
import scala.io._
 

object isPelindrome {
  def main(args:Array[String]):Unit={
        var s:String=scala.io.StdIn.readLine();
        s=s.toLowerCase();
        if(s==s.reverse){
            println("s is pelindromic");

        }
        else {
            println(s+" is not pelindromic");
        }
    }
}
