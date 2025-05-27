object PrimeNumberGenerator {
  def isPrime(num: Int): Boolean = {
    if (num <= 1) false
    else if (num <= 3) true
    else if (num % 2 == 0 || num % 3 == 0) false
    else {
      var i = 5
      while (i * i <= num) {
        if (num % i == 0 || num % (i + 2) == 0) return false
        i += 6
      }
      //print(num+ " ");
      true
    }
  }

 

  def main(args: Array[String]): Unit = {
    val n = scala.io.StdIn.readInt(); 
    for(i<-1 to n){
        if(isPrime(i)){
            print(i+ " ");
        }
    }
    
   
}
}