file://<WORKSPACE>/tryout.scala/ExceptionHandeling.scala
### dotty.tools.dotc.ast.Trees$UnAssignedTypeException: type of Select(Ident(cats),data) is not assigned

occurred in the presentation compiler.

action parameters:
uri: file://<WORKSPACE>/tryout.scala/ExceptionHandeling.scala
text:
```scala
import scala.io.StdIn
import scala.util.{Try, Success, Failure}
package ExceptionHandeling
import cats.data.Validated
object scala {

  ///1
  val magicChar=try{
    val scala:String="Scala"
    scala.charAt(20)
  }
  catch {
    case e:NullPointerException=>'z'
    case r:RuntimeException=>'s'

  }finally{
    //code to cleanup resources

  }
/*
* Pros
* -it  is an expression (unlike java)
* cans:
* -cumbersome, hard to read
* -nesting is disgusting
* finally and side effects
* */



  val aTry=Try(2) //Try.apply(2 =success(2)
  val aFail:Try[String]=Try{
    val scala:String="scala"
    scala.charAt(20)
  }
  /** Pros over ry/catch
    * -we care about the value not the exception
    * -map,flatMap, filter, for-comprehensions
    * -pattern matchig
    */

    val aModifiedTry=aTry.map(_ + 2)
    val aRecoveredFailure=aFail.recover{
        case e:RuntimeException=>'z'
    }

    val aChainedComputation=for{
        x<-aModifiedTry
        y<- aRecoveredFailure
    }yield(x,y)

import scala.util.Either
val aRight:Either[String,Int]=Right(22)
val aModifiedRight=aRight.map(_ + 1)

/**
  * pros over try
  * -error can be of any type
  * 
  * 
  * for comprehensions
  * APIs for manipulating left/right
  * 
  */

  //validated


val aValidValue:Validated[String,Int]=Validated.valid(42)//right
val aanInvalidValue:Validated[String,Int]=Validated.invalid("something went wrong")//left
val anTest:Validated[String,Int]=Validated.cond(42>39,23,"something went wrong")

def validatePositive(n:Int):Validated[List[String],Int]={
    Validated.cond(n>0,n,List(" Number must be positive"))
}

def validateSmall(n:Int):Validated[List[String],Int]={
    Validated.cond(n<100,n,List(" Number must be smaller than 100"))
}
def validatelEven(n:Int):Validated[List[String],Int]={
    Validated.cond(n%2==0,n,List(" Number must be even"))
}
import  cats.instances.list._ //implicit semigrpup pf lists which concatenates them
implicit val combineIntMax:semigrpup[Int]=something.instance[Int](Math.Max)

def validate (n:Int):validated[List[String],Int]={
    validatePositive(n)
    .combine(validateSmall(n))
    .combine(validatelEven(n))
}
   def main(args: Array[String]): Unit = {

  }
}

```



#### Error stacktrace:

```
dotty.tools.dotc.ast.Trees$Tree.tpe(Trees.scala:71)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse$$anonfun$15(ExtractSemanticDB.scala:251)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:15)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:10)
	scala.collection.immutable.List.foreach(List.scala:333)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:258)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse$$anonfun$1(ExtractSemanticDB.scala:142)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:15)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:10)
	scala.collection.immutable.List.foreach(List.scala:333)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:142)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse$$anonfun$1(ExtractSemanticDB.scala:142)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:15)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:10)
	scala.collection.immutable.List.foreach(List.scala:333)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:142)
	scala.meta.internal.pc.SemanticdbTextDocumentProvider.textDocument(SemanticdbTextDocumentProvider.scala:38)
	scala.meta.internal.pc.ScalaPresentationCompiler.semanticdbTextDocument$$anonfun$1(ScalaPresentationCompiler.scala:178)
```
#### Short summary: 

dotty.tools.dotc.ast.Trees$UnAssignedTypeException: type of Select(Ident(cats),data) is not assigned