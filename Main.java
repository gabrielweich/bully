public class Main {
    public static void main(String[] args) {
        try {
            new Node().start(args[0], Integer.parseInt(args[1]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}