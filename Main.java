public class Main {
    public static void main(String[] args) {
        if (args.length != 2){
            System.out.println("Uso: java Main <config> <linha>");
            return;
        }
        try {
            new Node().start(args[0], Integer.parseInt(args[1]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}