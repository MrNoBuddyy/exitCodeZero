
import scala.collection.immutable._


object PatternMatching {
  //first use  -switch on steroids
  val num = 44
  val ordinal = num match {
    case 1 => "first"
    case 2 => "second"
    case 3 => "third"
    case _ => num + "th"
  }
  /// second case class deconstruction
  case class Person(name:String,age:Int)
  val bob=Person("Bob",21)
  val bobGreeting=bob match{
    case Person(n,a)=>s"hi my name is $n and i am $a year old"
  }
  //trick #1 -list extractors
  val numberList=List(1,2,3,4)
  val mustHave=numberList match{
    case List(_,_,3,somethingElse)=>s"List has 3rd element 3 so the 4th elment is $somethingElse"
  }
  //#2-haskell -like prepending
  val startsWIthOne=numberList match{
    case Nil =>"the list is empty"
    case 1::tail=>s"list starts with the one, tail is $tail"
   // case ::(1,tail) =>s"list starts with the one, tail is $tail"  // this exactly same as above one
  }
  def process(aList:List[Int])=aList match{
    case head::tail=>s"list starts with $head, tail is $tail"
  }
  //#3 varag pattern
  val donstCareAboutTheRest=numberList match{
    case List(_,2,_*)=>"I only care about the second number being 2"
  }
  //#4 other infix patterns
//  val mustEndWithMeaningOfLife=numberList match{
//    case List(_,2,_):+42=>"that's right, i have a meaning"
//  }
//  val mustEndWithMeaningOfLife2=numberList match{
//    case List(1,_*):+42=>"i dont care how long the list is, i just want to end with 42"
//  }
  //#5 type specifiers
  def giveMeAValue():Any=45
  val giveMeTheType=giveMeAValue()match{
    case _:String=>"i hve a string" //'_:String' these are also called type guards and and are base on reflections, so it is likely to hit the performance
    case _:Int=>"i have an int"
    case _=>"something else"
  }
  //#6 name binding
  def requestMoreInfo(p:Person):String=s"the person ${p.name} is a good person"
  val bobsInfo=bob match {
    case Person(n,a)=>s"$n's info : ${requestMoreInfo(Person(n,a))}"
    //case p @ Person(n,a)=>s"$n's info : ${requestMoreInfo(p)}"  this to works fine
  }
  //#7 conditional causes
  val ordinal2=num match {
    case 1 => "first"
    case 2 => "second"
    case 3 => "third"
    case n if n%10==1=>n+"st"
    case n if n%10==2=>n+"nd"
    case n if n%10==3=>n+"rd"
    case _=> num+"th"
  }
  //#8 alternative patterns
  val myOptimalList=numberList match {
    case List(1,_*)|List(_,_,3,_*)=>"i like this list"    //instead of writing two different cases we can use or operator in single case
    //case List(_,_,3,_*)=>"i like this list"
    case _=>"i hate this list"
  }
  def main(args: Array[String]): Unit = {
    println(myOptimalList)
  }
}
