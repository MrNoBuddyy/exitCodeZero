import java.io._
import scala.io._
import scala.collection.immutable.ListMap
//Define a map of type (String, Int), sort the map based on the value in ascending order

object tryOne{
    def main(args:Array[String]):Unit={
        var map=Map( 
            "a"->1,
            "b"->2,
            "c"->3,
            "z"->26,
            "i"->9,
            "m"->13
        )
        
        print(ListMap(map.toSeq.sortWith(_._2 >_._2):_*)) ;
    }
}







//Create a class Person (attributes - name, age, address (previous problem class)) - (methods - getter and setter for each attribute)
// class Address{
//     protected var city:String=" ";
//     protected   var country:String=" ";
//     def setCity(city:String):Unit={
//         this.city=city;
//     }
//     // Address(city:String,country:String){
//     //     this.city=city;
//     //     this.country=country;
//     // }
//     def setCountry(country:String):Unit={
//         this.country=country;
//     }
//     def getCity():String={
//         return this.city;
//     }
//     def getCountry():String={
//         return this.country;
//     }
// }

// class Person extends Address{
//     protected  var name:String=" ";
//     protected  var age:Int=0;
//     // Person()
//     //  def setName(name:String):Unit={
//     //     this.name=name;
//     //     this.age=age;
//     // }
//     def setName(name:String):Unit={
//         this.name=name;
//     }
//      def setAge(age:Int):Unit={
//         this.age=age;
//     }
//     def getName():String={
//         return this.name;
//     }
//     def getAge():Int={
//         return this.age;
//     }
//      def introPerson(person:Person):Unit={
//             println(person.getName());
//             println(person.getAge());
//             println(person.getCity());
//             println(person.getCountry());
//         } 
// }
// object tryOne{
//     def main(args:Array[String]):Unit={
//         var name=StdIn.readLine();
//         var age=StdIn.readInt();
//         var city=StdIn.readLine();
//         var country=StdIn.readLine();
//         var buddy=new Person();
//         buddy.setName(name);
//         buddy.setAge(age);
//         buddy.setCity(city);
//         buddy.setCountry(country);
        
//         buddy.introPerson(buddy);

//     }
// }
























// def fib(cur:Int,n:Int,a:Long,b:Long):Unit={
//     if(cur<n){
//         print(a+" ");
//     }
//     else {return;}
//     fib(cur+1,n,b,a+b);
// }


// object  tryOne{
//     def main(args:Array[String]):Unit={
//         var n:Int=scala.io.StdIn.readInt();
//         fib(0,n,0,1);
//         }



//     }







        // var fab1:Long=0;
        // var fab2:Long=1;
        // for(i<-1 to n){
        //     print(fab1+" ");
        //     var sum:Long=fab1+fab2;
        //     fab1=fab2;
        //     fab2=sum;
        // }
        // println();
 //















// def fact(n:Long):Long={
//     if(n==1)return 1;
//     return n*fact(n-1);

// }


// object tryOne{
//     def main(args:Array[String])={
//         var n:Long=scala.io.StdIn.readLong();
//         println(fact(n));

//         //var fac:Int=1;

//         // for(i <-1 to n){
//         //     fac*=i;
//         // }
//         // println(fac);
//     }
// }









// class Point(var xc: Int, var yc: Int) {
//    var x: Int = xc
//    var y: Int = yc
   
//    def move(dx: Int, dy: Int)= {
//       x = x + dx
//       y = y + dy
//     //   println ("Point x location : " + x);
//     //   println ("Point y location : " + y);
//    }
// }

// class Location(override var xc: Int, override var yc: Int,
//    var zc :Int) extends Point(xc, yc){
//    var z: Int = zc

//    def move(dx: Int, dy: Int, dz: Int) ={
//       x = x + dx
//       y = y + dy
//       z = z + dz
//       println ("Point x location : " + x);
//       println ("Point y location : " + y);
//       println ("Point z location : " + z);
//    }
// }

// object Demo {
//    def main(args: Array[String])= {
//       var loc = new Location(10, 20, 15);

//       // Move to a new location
//       var a:Int = scala.io.StdIn.readInt();
//       //println(a);
//       var b:Int = scala.io.StdIn.readInt();
//      // println(b);
//       var c:Int = scala.io.StdIn.readInt();
//       //println(c);
//       //var num:Int = scala.io.StdIn.readInt();
//       //println(num);
//       loc.move(a,b,c);
//    }
// }