import java.io._
import scala.io._

class Address { // Address class with the protected attributes city and country accessed by the help of getters and setters 

    protected var city:String=" ";
    protected   var country:String=" ";
    def setCity(city:String):Unit={
        this.city=city;
    }
    def setCountry(country:String):Unit={
        this.country=country;
    }
    def getCity():String={
        return this.city;
    }
    def getCountry():String={
        return this.country;
    }
    // def display():Unit={
    //     println(this.getCity());
    //     println(this.getCountry());
    // }
}
class Person extends Address() {
    private  var name:String=" ";
    private  var age:Int=0;
    
    
    def setName(name:String):Unit={
        this.name=name;
    }
     def setAge(age:Int):Unit={
        this.age=age;
    }
    def getName():String={
        return this.name;
    }
    def getAge():Int={
        return this.age;
    }
     def introPerson(person:Person):Unit={
            println(person.getName());
            println(person.getAge());
            println(person.getCity());
            println(person.getCountry());
        } 
        // def display():Unit={
        //     println(this.getName());
        //     println(this.getAge());
            
        // }
}

object ID{
    def main(args:Array[String]):Unit={
        var obj=new Person();
        var name=StdIn.readLine();
        var age=StdIn.readInt();
        var city=StdIn.readLine();
        var country=StdIn.readLine();
        obj.setName(name);
        obj.setAge(age);
        obj.setCity(city);
        obj.setCountry(country);
        obj.introPerson(obj);
    }
}