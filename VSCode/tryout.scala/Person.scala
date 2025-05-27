class Person {
    private  var name:String=" ";
    private  var age:Int=0;
    def this() {
        this("", 0) ;// Default constructor
    }
    
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
