  import java.io._
import scala.io._
import scala.collection.immutable.ListMap

///method to sort the map

object SortMap {




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

