package args;

public class args {
    public static void main(String args[]) {
        System.out.println(args[0]);
        int s = 0;
        for (int i = 0; i < args.length; i++) {
            s += Integer.parseInt(args[i]);
        }
        System.out.println(s);
    }
}