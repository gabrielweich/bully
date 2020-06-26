import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;


public class Messenger {
    //Envia um mensagem a um nodo
    public static boolean sendMessage(DatagramSocket socket, SocketAddress address, String message) {
        byte[] command = new byte[1024];
        command = message.getBytes();
        try {
            socket.send(new DatagramPacket(command, command.length, address));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    //Envia uma mensagem a um nodo perguntando se está vivo e aguarda resposta
    public static boolean isAlive(SocketAddress address) {
        try {
            DatagramSocket socket = new DatagramSocket();
            Messenger.sendMessage(socket, address, "alive");
            Messenger.receive(socket, 1000);
            socket.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    //Extrai a mensagem de um pacote UDP
    public static String extractMessage(DatagramPacket packet) {
        return new String(packet.getData(), 0, packet.getLength());
    }

    //Recebe um pacote UDP em um dado timeout
    public static DatagramPacket receive(DatagramSocket socket, int timeout) throws IOException {
        byte[] text = new byte[1024];
        DatagramPacket packet = new DatagramPacket(text, text.length);
        socket.setSoTimeout(timeout);
        socket.receive(packet);
        return packet;
    }
}