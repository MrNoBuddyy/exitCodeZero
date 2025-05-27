class Book(val title:String,val author:String,val publicationYear:Int ){
  def this(title:String,author:String) {///auxiliary constructor to initialize a Book object in case the publication year in not available
    this(title,author,0)
  }
  def describe(): Unit = {  //method printing the details of Book class objects
    if(publicationYear!=0){
      println(s"This book $title is written by $author and was published in the year $publicationYear")  //conditional method for the books which has unknown publication year
    }
    else{
      println(s"This book $title is written by $author ")
    }
  }
}
object Main{
  def main(args: Array[String]): Unit = {
    var book1=new Book("Godaan","Munshi Premchand",1936)
    var book2=new Book("Surrounded by Idiots", "Thomas Erikson")
    book1.describe()
    book2.describe()
  }
}

//class Book(var title:String,val author:String,var publicationYear:Int) {
//  def this(title :String,author:String){
//    this(title,author,-1)
//  }
//  def details(): Unit = {
//    if(publicationYear>0) {
//      var dtls: String = if (publicationYear > 0) s"This book $title is written by $author and was published in the year $publicationYear" else s"This book $title is written by $author "
//    }
//    println(dtls)
//  if
//
//
//}
//object Main{
//  def main(args: Array[String]): Unit = {
//    val book1=new Book("Godaan", "Munshi Premchand",1936)
//    val book2=new Book("Surrounded by Idiots", "Thomas Erikson")
//    book1.details()
//    book2.details()
//  }
//}