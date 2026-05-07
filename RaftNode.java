import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.*;

public class RaftNode extends UnicastRemoteObject
        implements RaftNodeInterface {

    enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private int id;
    private State state;

    private int currentTerm = 0;
    private int votedFor = -1;

    private long lastHeartbeatTime;

    private Map<String, String> store = new HashMap<>();
    private List<String> log = new ArrayList<>();

    private Map<Integer, Integer> peers;

    protected RaftNode(int id,
                       Map<Integer, Integer> peers)
            throws RemoteException {

        this.id = id;
        this.peers = peers;

        state = State.FOLLOWER;

        lastHeartbeatTime = System.currentTimeMillis();

        startElectionTimer();
    }

    // ===================================
    // Election Timer
    // ===================================

    private void startElectionTimer() {

        new Thread(() -> {

            while (true) {

                try {

                    int timeout =
                            new Random().nextInt(3000) + 3000;

                    Thread.sleep(500);

                    long now = System.currentTimeMillis();

                    if (state != State.LEADER &&
                            now - lastHeartbeatTime > timeout) {

                        startElection();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }).start();
    }

    // ===================================
    // Start Election
    // ===================================

    private synchronized void startElection() {

        state = State.CANDIDATE;

        currentTerm++;

        votedFor = id;

        int votes = 1;

        System.out.println(
                "Node " + id +
                        " started election for term "
                        + currentTerm);

        for (Map.Entry<Integer, Integer> peer
                : peers.entrySet()) {

            try {

                int peerId = peer.getKey();
                int peerPort = peer.getValue();

                Registry registry =
                        LocateRegistry.getRegistry(
                                "localhost",
                                peerPort);

                RaftNodeInterface node =
                        (RaftNodeInterface)
                                registry.lookup(
                                        "Node" + peerId);

                boolean vote =
                        node.requestVote(
                                currentTerm,
                                id);

                if (vote) {
                    votes++;
                }

            } catch (Exception e) {

                System.out.println(
                        "Node " + peer.getKey()
                                + " unreachable");
            }
        }

        if (votes >= 2) {

            becomeLeader();

        } else {

            state = State.FOLLOWER;
        }
    }

    // ===================================
    // Become Leader
    // ===================================

    private void becomeLeader() {

        state = State.LEADER;

        System.out.println(
                "Node " + id +
                        " became LEADER");

        startHeartbeat();
    }

    // ===================================
    // Heartbeat
    // ===================================

    private void startHeartbeat() {

        new Thread(() -> {

            while (state == State.LEADER) {

                try {

                    for (Map.Entry<Integer, Integer>
                            peer : peers.entrySet()) {

                        try {

                            int peerId = peer.getKey();
                            int peerPort = peer.getValue();

                            Registry registry =
                                    LocateRegistry.getRegistry(
                                            "localhost",
                                            peerPort);

                            RaftNodeInterface node =
                                    (RaftNodeInterface)
                                            registry.lookup(
                                                    "Node" + peerId);

                            node.appendEntries(
                                    currentTerm,
                                    id,
                                    new ArrayList<>());

                        } catch (Exception e) {

                            System.out.println(
                                    "Heartbeat failed to Node "
                                            + peer.getKey());
                        }
                    }

                    Thread.sleep(50);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }).start();
    }

    // ===================================
    // Vote Request
    // ===================================

    @Override
    public synchronized boolean requestVote(
            int term,
            int candidateId) {

        if (term > currentTerm) {

            currentTerm = term;

            votedFor = -1;

            state = State.FOLLOWER;
        }

        if (votedFor == -1 ||
                votedFor == candidateId) {

            votedFor = candidateId;

            System.out.println(
                    "Node " + id +
                            " voted for Node "
                            + candidateId);

            return true;
        }

        return false;
    }

    // ===================================
    // Append Entries
    // ===================================

    @Override
    public synchronized boolean appendEntries(
            int term,
            int leaderId,
            List<String> entries) {

        state = State.FOLLOWER;

        currentTerm = term;

        lastHeartbeatTime =
                System.currentTimeMillis();

        for (String entry : entries) {

            log.add(entry);

            String[] parts = entry.split("=");

            store.put(parts[0], parts[1]);
        }

        return true;
    }

    // ===================================
    // PUT
    // ===================================

    @Override
    public synchronized String put(
            String key,
            String value) {

        if (state != State.LEADER) {

            return "NOT_LEADER";
        }

        String entry = key + "=" + value;

        log.add(entry);

        int success = 1;

        for (Map.Entry<Integer, Integer>
                peer : peers.entrySet()) {

            try {

                int peerId = peer.getKey();
                int peerPort = peer.getValue();

                Registry registry =
                        LocateRegistry.getRegistry(
                                "localhost",
                                peerPort);

                RaftNodeInterface node =
                        (RaftNodeInterface)
                                registry.lookup(
                                        "Node" + peerId);

                boolean ok =
                        node.appendEntries(
                                currentTerm,
                                id,
                                Arrays.asList(entry));

                if (ok) {
                    success++;
                }

            } catch (Exception e) {

                System.out.println(
                        "Replication failed");
            }
        }

        if (success >= 2) {

            store.put(key, value);

            return "PUT SUCCESS";
        }

        return "PUT FAILED";
    }

    // ===================================
    // GET
    // ===================================

    @Override
    public String get(String key) {

        if (state != State.LEADER) {

            return "NOT_LEADER";
        }

        return store.getOrDefault(
                key,
                "NULL");
    }

    // ===================================
    // Is Leader
    // ===================================

    @Override
    public boolean isLeader() {

        return state == State.LEADER;
    }
}