import scala.io._
class Parent{
    def method1():Unit={
        println("this method1 is from parent class ")
    }
     def method2():Unit={
        println("this method2 is from parent class ")
    }
}
class Child extends Parent{
    override def method1():Unit={
        println("this method1 is from child class ")//simplest way to show method overriding , here method1 is present in Parent class and child class as well, so we have to override the method and can have it's diffrent body 
    }
     def method1(name:String):Unit={//the  argument list of child class method is different form parent class then method overloading is not required as methode
        println(s"Hii from child $name ")
    }
     override def method2():Unit={
        println("this method2 is from child class ")
    }
}


object methodOverriding {
 def main(args:Array[String]):Unit={
    val parent=new Parent();
    val child=new Child()
    parent.method1()
    parent.method2()
    child.method1()
    val name:String=scala.io.StdIn.readLine()
    child.method1(name)
    child.method2()
 }
}
