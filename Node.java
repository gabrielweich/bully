import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class NodeProperties {
    int id;
    SocketAddress address;

    public NodeProperties(int id, SocketAddress address) {
        this.id = id;
        this.address = address;
    }

    @Override
    public String toString() {
        return Integer.toString(this.id);
    }
}

class MessageProcessor extends Thread {
    DatagramSocket socket;
    Map<Integer, NodeProperties> nodes;
    NodeProperties currentNode;
    NodeProperties coordinator;
    long end = Long.MAX_VALUE;
    long lastCoordinatorCheck;
    long lastElectionStart;
    boolean inElection;

    public MessageProcessor(DatagramSocket socket, Map<Integer, NodeProperties> nodes, NodeProperties currentNode) {
        this.socket = socket;
        this.nodes = nodes;
        this.currentNode = currentNode;
    }

    public MessageProcessor(DatagramSocket socket, Map<Integer, NodeProperties> nodes, NodeProperties currentNode,
            NodeProperties coordinator) {
        this.socket = socket;
        this.nodes = nodes;
        this.currentNode = currentNode;
        this.coordinator = coordinator;
    }

    //Laço que processa as mensagens recebidas de outros nodos
    public void run() {
        if (this.isCurrentCoordinator())
            end = System.currentTimeMillis() + 10000;

        while (!Thread.interrupted() && System.currentTimeMillis() < end) {
            try {
                if (!this.inElection && this.isExternalCoordinator())
                    this.checkCoordinator();
                if (this.inElection)
                    this.checkElectionState();
                DatagramPacket packet = Messenger.receive(socket, 100);
                String message = Messenger.extractMessage(packet);
                System.out.println("received: " + message);
                if (message.startsWith("alive"))
                    this.processAlive(packet, message);
                else if (message.startsWith("election"))
                    this.processElection(packet, message);
                else if (message.startsWith("coordinator"))
                    this.processCoordinator(packet, message);
                else if (message.startsWith("confirm"))
                    this.processConfirm(packet, message);

            } catch (IOException e) {
            }
        }
    }

    //Verifica se o coordenador é outro nodo
    private boolean isExternalCoordinator() {
        return this.coordinator != null && this.coordinator.id != this.currentNode.id;
    }

    //Verifica se o nodo é o atual coordenador
    private boolean isCurrentCoordinator() {
        return this.coordinator != null && this.coordinator.id == this.currentNode.id;
    }

    //Se o coordenador está a mais de 3s sem responder o nodo torna-se coordenador
    private void checkCoordinator() {
        if (System.currentTimeMillis() - lastCoordinatorCheck > 3000) {
            if (Messenger.isAlive(this.coordinator.address))
                lastCoordinatorCheck = System.currentTimeMillis();
            else {
                System.out.println("t " + this.coordinator.id);
                this.coordinator = null;
                try {
                    this.callElection();
                } catch (SocketException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //Processa uma mensagem de um nodo que iniciou uma eleição respondendo que está vivo e inicia uma nova eleição
    private void processElection(DatagramPacket packet, String message) {
        Messenger.sendMessage(socket, packet.getSocketAddress(), "confirm");
        if (!this.inElection) {
            try {
                this.callElection();
            } catch (SocketException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    //Responde a uma mensagem perguntando se o nodo está vivo
    private void processAlive(DatagramPacket packet, String message) {
        Messenger.sendMessage(socket, packet.getSocketAddress(), "1");
    }

    //Processa uma mensagem de um nodo afirmando ser coordenador e atualiza o coordenador atual
    private void processCoordinator(DatagramPacket packet, String message) {
        int coordinatorId = Integer.parseInt(message.split(";")[1]);
        if (coordinatorId > this.currentNode.id) {
            this.coordinator = this.nodes.get(coordinatorId);
            System.out.println("c " + coordinatorId);
        }
    }

    //Processa uma mensagem de confirmação de um nodo durante o processo de eleição encerrando a eleição
    private void processConfirm(DatagramPacket packet, String message) {
        this.lastElectionStart = -1;
        this.inElection = false;
    }

    //Verifica se depois de 1s desde o início da eleição não obteve resposta e torna-se coordenador
    private void checkElectionState() {
        if (System.currentTimeMillis() - this.lastElectionStart > 1000) {
            this.inElection = false;
            try {
                this.coordinate();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    //Torna-se coordenador e estabelece um limite de 10s para execução do algoritmo 
    private void coordinate() throws InterruptedException {
        System.out.println("c " + this.currentNode.id);
        for (NodeProperties node : this.nodes.values()) {
            Messenger.sendMessage(socket, node.address, "coordinator;" + this.currentNode.id);
        }
        this.coordinator = this.currentNode;
        this.end = System.currentTimeMillis() + 10000;
    }

    //Inicia um processo de eleição enviando uma mensagem para os nodos com id maior
    private void callElection() throws SocketException, InterruptedException {
        this.inElection = true;
        this.lastElectionStart = System.currentTimeMillis();

        List<NodeProperties> greaterIdNodes = this.nodes.values().stream().filter(n -> n.id > this.currentNode.id)
                .collect(Collectors.toList());

        System.out.println("e " + Arrays.toString(greaterIdNodes.toArray()));

        for (NodeProperties node : greaterIdNodes) {
            Messenger.sendMessage(this.socket, node.address, "election;" + this.currentNode.id);
        }
    }
}

//Runnable para testar conexão com um nodo alvo
class NodeConnectRunnable implements Runnable {
    SocketAddress targetAddress;

    public NodeConnectRunnable(SocketAddress targetAddress) {
        this.targetAddress = targetAddress;
    }

    @Override
    public void run() {
        while (!Messenger.isAlive(targetAddress)) {
            System.out.println(this.targetAddress + " not responding");
        }
        System.out.println(this.targetAddress.toString() + " connected");
    }

}

class Node {
    DatagramSocket socket;
    Map<Integer, NodeProperties> nodes;
    NodeProperties currentNode;

    public Node() throws IOException {
        this.nodes = new ConcurrentHashMap<>();
    }

    //Lê o arquivo de configuração atualizando o mapeamento de nodos
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

    //Encontra o nodo com maior id para ser o coordenador inicial
    private NodeProperties getFirstCoordinator() {
        NodeProperties first = this.currentNode;
        for (NodeProperties node : this.nodes.values()) {
            if (node.id > first.id)
                first = node;
        }
        return first;
    }

    //Instancia um socket UDP
    private void connect() throws SocketException {
        SocketAddress address = this.currentNode.address;
        this.socket = new DatagramSocket(address);
        System.out.println("Connected at " + address);
    }

    //Aguarda obter resposta de todos os nodos
    private void waitAllNodesConnect() throws InterruptedException {
        ExecutorService es = Executors.newCachedThreadPool();
        for (NodeProperties target : this.nodes.values()) {
            es.execute(new NodeConnectRunnable(target.address));
        }
        es.shutdown();
        while (!es.awaitTermination(2, TimeUnit.MINUTES)) {
        }
    }

    //Inicia a execução do algoritmo
    public void start(String configFile, int lineNumber) throws Exception {
        this.readConfig(configFile, lineNumber);
        this.connect();
        Thread messageProcessor = new MessageProcessor(socket, nodes, currentNode);
        messageProcessor.start();
        this.waitAllNodesConnect();
        messageProcessor.interrupt();
        NodeProperties firstCoordinator = this.getFirstCoordinator();
        System.out.println("c " + firstCoordinator.id);
        messageProcessor = new MessageProcessor(socket, nodes, currentNode, firstCoordinator);
        messageProcessor.start();
        messageProcessor.join();
        this.socket.close();
    }
}