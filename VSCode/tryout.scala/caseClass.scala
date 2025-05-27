
// case classs is primarily used for immutable data data modeling
//the parameters of case class are automatically treated as immutable
//default 'toString','equals' and 'hashCode'methods =>automatic implementation of these methodes based on their fields
//a companion object is automatically created for the case class  and it includes an apply method that allows us to to create instance of the case class without using the new keyword
object caseClass {
  case class Rectangle(length:Double,width:Double){//a case class of type rectangle which has two methods area and isSquare
    def area():Double={
        length*width
    }
    def isSquare():Boolean={
        length==width
    }
  }
  def main(aegs:Array[String]):Unit={
    val rectangle1=Rectangle(4,3)
    val rectangle2=Rectangle(6,2)
    val rectangle3=Rectangle(4,4)

    println(s"area of rectangle1 is ${rectangle1.area()}, is it Square? ${rectangle1.isSquare()}")
    println(s"area of rectangle2 is ${rectangle2.area()}, is it Square? ${rectangle2.isSquare()}")
    println(s"area of rectangle1 is ${rectangle3.area()}, is it Square? ${rectangle3.isSquare()}")

   
  }
}
