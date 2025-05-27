//import scala.io.StdIn
//import scala.util.{Try, Success, Failure}
//package ExceptionHandeling
//
//object scala {
//
//  ///1
//  val magicChar=try{
//    val scala:String="Scala"
//    scala.charAt(20)
//  }
//  catch {
//    case e:NullPointerException=>'z'//basically in try catch finally we are trying to execute some task and based on some known particular results wi will give some output
//    case r:RuntimeException=>'s'
//
//  }finally{
//    //code to cleanup resources
//
//  }
///*
//* Pros
//* -it  is an expression (unlike java)
//* cans:
//* -cumbersome, hard to read
//* -nesting is disgusting
//* finally and side effects
//* */
//
//
//  import scala._
//  val aTry=Try(2) //Try.apply(2 =success(2)
//   def main(args: Array[String]): Unit = {
//
//  }
//}
