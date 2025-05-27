
class GFG {
    public static void concat1(String s1){
        s1=s1+" buddy!!";
    }
    public static void concat2(StringBuilder s){
        s.append(" buddy!!");
    }
    public static void main (String[] args) {
      
          // String literal
          String s1="Hello";
          System.out.println(s1);
          concat1(s1);
          System.out.println(s1);
        
          // Using new Keyword 
          StringBuilder s2= new StringBuilder("Hello");
          concat2(s2);
          System.out.println(s2);
    }
}