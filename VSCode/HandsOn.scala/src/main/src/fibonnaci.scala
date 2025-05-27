import java.io._
import scala.io._
 





object fibonnaci {
    def fibRec(cur:Int,n:Int,a:Long,b:Long):Unit={  ///method to print first n fibonnaci numbers recursively
        if(cur<n){
            print(a+" ");
        }
        else {return;}
        fibRec(cur+1,n,b,a+b);
    }
    def FibLoop(n:Int):Unit={    /// method to print first n fibonnaci numbers iteratively
        var a:Long=0;
        var b:Long=1;
        for(i<-1 to n){
            print(a+" ");
            var temp:Long=a+b;
            a=b;
            b=temp;
        }
        println();
    }

    def main(args:Array[String]):Unit={
    var n:Int=scala.io.StdIn.readInt();
    FibLoop(n);
    fibRec(0,n,0,1);
    }
}
