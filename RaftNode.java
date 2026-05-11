import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.*;
import java.io.*;

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

    // Logs
    private List<String> log = new ArrayList<>();

    // commitIndex = highest log index known to be committed.
    // lastApplied = highest log index applied to store.
    // Both are 0-based; -1 means nothing committed/applied yet.
    private int commitIndex = -1;
    private int lastApplied  = -1;

    private Map<Integer, Integer> peers;

    protected RaftNode(int id,
                       Map<Integer, Integer> peers)
            throws RemoteException {

        this.id = id;
        this.peers = peers;

        state = State.FOLLOWER;

        lastHeartbeatTime =
                System.currentTimeMillis();

        // Restore persisted state (currentTerm, votedFor, log)
        // before joining the cluster so we don't lose committed
        // entries across restarts.
        loadState();

        startElectionTimer();
    }

    // ===================================
    // Persistence
    // ===================================

    private String stateFile() {
        return "node_" + id + "_state.txt";
    }

    private synchronized void saveState() {

        try (PrintWriter pw =
                     new PrintWriter(stateFile())) {

            pw.println(currentTerm);
            pw.println(votedFor);

            for (String entry : log) {
                pw.println(entry);
            }

        } catch (Exception e) {

            System.out.println(
                    "Node " + id +
                            " failed to save state: "
                            + e.getMessage());
        }
    }

    private void loadState() {

        File f = new File(stateFile());

        if (!f.exists()) return;

        try (Scanner sc = new Scanner(f)) {

            currentTerm =
                    Integer.parseInt(sc.nextLine().trim());

            votedFor =
                    Integer.parseInt(sc.nextLine().trim());

            while (sc.hasNextLine()) {

                String line = sc.nextLine().trim();

                if (!line.isEmpty()) {
                    log.add(line);
                }
            }

            // lastApplied stays -1 here — entries will be
            // re-applied once the new leader sends its
            // commitIndex via heartbeat, so we never apply
            // anything that wasn't confirmed as committed.
            System.out.println(
                    "Node " + id +
                            " restored " + log.size() +
                            " log entries from disk" +
                            " (term=" + currentTerm +
                            ", votedFor=" + votedFor + ")");

        } catch (Exception e) {

            System.out.println(
                    "Node " + id +
                            " failed to load state: "
                            + e.getMessage());
        }
    }

    // ===================================
    // Election Timer
    // ===================================

    private void startElectionTimer() {

        new Thread(() -> {

            while (true) {

                try {

                    // Draw the timeout ONCE per attempt and
                    // sleep the full duration before checking —
                    // not a new random on every 500 ms tick.
                    int timeout =
                            new Random().nextInt(3000)
                                    + 3000;

                    Thread.sleep(timeout);

                    if (state != State.LEADER &&
                            System.currentTimeMillis()
                                    - lastHeartbeatTime
                                    >= timeout) {

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

        // Persist before sending any RPCs so we never
        // vote twice in the same term after a crash.
        saveState();

        int votes = 1;

        System.out.println(
                "Node " + id +
                        " started election for term "
                        + currentTerm);

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

                boolean vote =
                        node.requestVote(
                                currentTerm,
                                id);

                if (vote) {

                    votes++;
                }

            } catch (Exception e) {

                System.out.println(
                        "Node "
                                + peer.getKey()
                                + " unreachable");
            }
        }

        if (votes >= 2) {

            becomeLeader();

        } else {

            // Split-vote back-off — step down to FOLLOWER
            // and reset lastHeartbeatTime so the election
            // timer counts a full fresh timeout before
            // trying again, avoiding rapid re-election storms.
            state = State.FOLLOWER;

            lastHeartbeatTime =
                    System.currentTimeMillis();
        }
    }

    // ===================================
    // Become Leader
    // ===================================

    private void becomeLeader() {

        state = State.LEADER;

        System.out.println(
                "Node " + id
                        + " became LEADER");

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

                            int peerId =
                                    peer.getKey();

                            int peerPort =
                                    peer.getValue();

                            Registry registry =
                                    LocateRegistry
                                            .getRegistry(
                                                    "localhost",
                                                    peerPort);

                            RaftNodeInterface node =
                                    (RaftNodeInterface)
                                            registry.lookup(
                                                    "Node"
                                                            + peerId);

                            node.appendEntries(
                                    currentTerm,
                                    id,
                                    new ArrayList<>(),
                                    commitIndex);

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
    // Request Vote
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

            // Persist votedFor so we never vote twice
            // in the same term after a crash.
            saveState();

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
            List<String> entries,
            int leaderCommit) {

        // Reject stale-term calls — a former leader with
        // an old term must not reset our timer or overwrite
        // our currentTerm downward.
        if (term < currentTerm) {

            return false;
        }

        // Valid leader — accept and reset timer.
        state = State.FOLLOWER;

        currentTerm = term;

        lastHeartbeatTime =
                System.currentTimeMillis();

        // Update our commitIndex from the leader so we
        // know how far we can apply.
        if (leaderCommit > commitIndex) {

            commitIndex = Math.min(
                    leaderCommit,
                    log.size() - 1);
        }

        for (String entry : entries) {

            // Only append to the log — do NOT apply to
            // the store yet. Application happens below
            // once we know what the leader has committed.
            log.add(entry);

            System.out.println(
                    "Node " + id +
                            " logged (not yet applied): "
                            + entry);
        }

        // Persist log and term before replying so entries
        // survive a crash between ack and commit.
        if (!entries.isEmpty()) {
            saveState();
        }

        // Advance lastApplied up to commitIndex so entries
        // the leader has committed get applied to our store.
        if (commitIndex > lastApplied) {

            for (int i = lastApplied + 1;
                 i <= commitIndex && i < log.size();
                 i++) {

                String[] parts =
                        log.get(i).split("=", 2);

                store.put(parts[0], parts[1]);

                lastApplied = i;

                System.out.println(
                        "Node " + id +
                                " applied committed log["
                                + i + "]: "
                                + log.get(i));
            }
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

        String entry =
                key + "=" + value;

        // Add to leader log and persist before
        // sending to followers.
        log.add(entry);

        saveState();

        System.out.println(
                "Leader Node " + id +
                        " added log: "
                        + entry);

        int success = 1;

        for (Map.Entry<Integer, Integer>
                peer : peers.entrySet()) {

            try {

                int peerId =
                        peer.getKey();

                int peerPort =
                        peer.getValue();

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
                                Arrays.asList(entry),
                                commitIndex);

                if (ok) {

                    success++;
                }

            } catch (Exception e) {

                System.out.println(
                        "Replication failed");
            }
        }

        // Majority success — advance commitIndex and
        // apply newly committed entries to leader store.
        if (success >= 2) {

            commitIndex = log.size() - 1;

            for (int i = lastApplied + 1;
                 i <= commitIndex;
                 i++) {

                String[] parts =
                        log.get(i).split("=", 2);

                store.put(parts[0], parts[1]);

                lastApplied = i;
            }

            System.out.println(
                    "Leader Node " + id +
                            " committed log: "
                            + entry);

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
