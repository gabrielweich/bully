import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


class Coordinate implements Callable<Void> {
    @Override
    public Void call() throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
        }
    }
}

class NodeProperties {
    int id;
    SocketAddress address;

    public NodeProperties(int id, SocketAddress address) {
        this.id = id;
        this.address = address;
    }
}


class NodeMessageListener extends Thread {
    DatagramSocket socket;
    Map<Integer, NodeProperties> nodes;
    int nodeId;

    public NodeMessageListener(DatagramSocket socket, Map<Integer, NodeProperties> nodes, int nodeId) {
        this.socket = socket;
        this.nodes = nodes;
        this.nodeId = nodeId;
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] texto = new byte[1024];
                DatagramPacket packet = new DatagramPacket(texto, texto.length);
                socket.setSoTimeout(2000);
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength());
                if (message.startsWith("alive"))
                    Messenger.sendMessage(socket, packet.getSocketAddress(), "1");
            } catch (IOException e) {
            }
        }
    }
}

class NodeConnectRunnable implements Runnable {
    SocketAddress targetAddress;
    int nodeId;
    int targetId;
    Map<Integer, NodeProperties> nodes;

    public NodeConnectRunnable(SocketAddress targetAddress, int nodeId, int targetId,
            Map<Integer, NodeProperties> nodes) {
        this.targetAddress = targetAddress;
        this.nodeId = nodeId;
        this.targetId = targetId;
        this.nodes = nodes;
    }

    @Override
    public void run() {
        while (!Messenger.isAlive(targetAddress)) {
            System.out.println(this.targetAddress + " not responding.");
        }
        System.out.println(this.targetAddress.toString() + " connected.");
    }

}

class Node {
    DatagramSocket socket;
    Map<Integer, NodeProperties> nodes;
    NodeProperties currentNode;

    public Node() throws IOException {
        this.nodes = new ConcurrentHashMap<>();
    }

    private void readConfig(String filename, int lineNumber) throws FileNotFoundException {
        File myObj = new File(filename);
        Scanner myReader = new Scanner(myObj);
        int currentLine = 1;
        while (myReader.hasNextLine()) {
            String[] tokens = myReader.nextLine().split(" ");
            int nodeId = Integer.parseInt(tokens[0]);
            int nodePort = Integer.parseInt(tokens[2]);
            SocketAddress nodeAddr = new InetSocketAddress(tokens[1], nodePort);
            NodeProperties nodeProperties = new NodeProperties(nodeId, nodeAddr);
            if (currentLine == lineNumber)
                this.currentNode = nodeProperties;
            else
                nodes.put(nodeId, nodeProperties);
            currentLine++;
        }
        myReader.close();
    }

    private boolean waitAllNodesConnect() throws InterruptedException {
        ExecutorService es = Executors.newCachedThreadPool();
        for (NodeProperties target : this.nodes.values()) {
            es.execute(new NodeConnectRunnable(target.address, this.currentNode.id, target.id, this.nodes));
        }
        es.shutdown();
        return es.awaitTermination(2, TimeUnit.MINUTES);
    }

    private NodeProperties getFirstCoordinator() {
        NodeProperties first = this.currentNode;
        for (NodeProperties node : this.nodes.values()) {
            if (node.id > first.id)
                first = node;
        }
        return first;
    }

    private void connect() throws SocketException {
        SocketAddress address = this.currentNode.address;
        this.socket = new DatagramSocket(address);
        System.out.println("Connected at " + address);
    }

    private void coordinate() throws InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.invokeAll(Arrays.asList(new Coordinate()), 10, TimeUnit.SECONDS);
        executor.shutdown();
    }

    private void monitor(NodeProperties coordinator) throws InterruptedException {
        while (Messenger.isAlive(coordinator.address)) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
            }
        }
    }

    public void start(String configFile, int lineNumber) throws Exception {
        this.readConfig(configFile, lineNumber);
        this.connect();
        Thread listener = new NodeMessageListener(this.socket, this.nodes, this.currentNode.id);
        listener.start();
        this.waitAllNodesConnect();
        NodeProperties coordinator = this.getFirstCoordinator();
        if (coordinator.id == this.currentNode.id) {
            this.coordinate();
        } else {
            this.monitor(coordinator);
        }

        this.socket.close();
        listener.interrupt();
    }
}