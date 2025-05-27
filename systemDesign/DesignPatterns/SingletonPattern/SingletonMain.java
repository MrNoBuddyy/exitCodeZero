class  Singleton {
    private static Singleton instance;
    private Singleton(){
        System.out.println("created Singleton instance");
    }
    public static Singleton getInstance(){
        if(instance==null){
            // Synchronized(Singleton.class);{
                            if(instance==null){
                                instance = new Singleton();
                            }
                        // }
                    }
                    return instance;
                }
    private static void Synchronized(Class<Singleton> class1) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'Synchronized'");
    }
    
}
public  class SingletonMain {

    public static void main(String[] args) {
        Singleton instance = Singleton.getInstance();
    }
}