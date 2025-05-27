import scala.collection.mutable.ListBuffer
import scala.collection.mutable
//generic allows us to create methods functions classes traits etc which can operate on different data types
// which makes code reusable 
//by creating a simple class using generics we can use it for Int, String, char and so on/

class Stack[T]{
    private val stack=ListBuffer.empty[T]
    def push(item:T):Unit={  //stack push function
        stack+=item
    }

    def pop():Option[T]={  //pop function 
        if(isEmpty())None
        else{
            val topItem=stack.last
            stack.remove(stack.length-1)
            Some(topItem)
        }
    }

    def top():Option[T]={ ///method to return the top os the stack
        if(isEmpty())None
        else{
            Some(stack.last)
        }
    }
    def size():Int={   //size of the stack
        stack.length
    }
    def isEmpty():Boolean={  // checks is the stack is empty or not
        if(stack.length==0)return true
        return false
    }


}
    object Main{
        def main(args :Array[String]):Unit={
            var stack=new Stack[Int]
            stack.push(1); //pushing elements onto the stack
            stack.push(3)
            stack.push(5)
            stack.push(7)

            println(s"stack size is ${stack.size()}")//prints the size of the stack
            println(s"Top of the stack is ${stack.top()}")//prints the top of the stack 

            println(s"Pop stack ${stack.pop().getOrElse("Stack is empty")}")// pops out the stacks top element 
            println(s"Pop stack ${stack.pop().getOrElse("Stack is empty")}")
            println(s"Pop stack ${stack.pop().getOrElse("Stack is empty")}")
            println(s"Pop stack ${stack.pop().getOrElse("Stack is empty")}")

            println(s"is stack empty ${ stack.isEmpty()}")// is empty boolean


        }
    }