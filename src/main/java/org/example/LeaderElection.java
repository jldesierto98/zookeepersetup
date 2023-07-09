package org.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private static final String TARGET_ZNODE = "/target_znode";
    private String currentZnodeName;
    private ZooKeeper zookeeper;


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZooKeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.reElectLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from zookeeper.");
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        // /c = candidate
        String znodePrefix = ELECTION_NAMESPACE + "/c";

        //if we disconnected from zookeeper, the znode will be deleted.
        String znodeFullPath = zookeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reElectLeader() throws InterruptedException, KeeperException {

        Stat predecessorStat = null;
        String predecessorZnodeName = "";

        while(predecessorStat == null){

            List<String> children = zookeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);

            String smallesChild = children.get(0);

            if(smallesChild.equals(currentZnodeName)){
                System.out.println("I am the leader!");
                return;
            }else{
                System.out.println("I am not the leader");
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zookeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }

        }

        System.out.println("Watching znode " + predecessorZnodeName);
        System.out.println();
//        System.out.println("I am not the leader, " + smallesChild + " is the leader");
    }

    public void connectToZooKeeper() throws IOException {
        this.zookeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException{
        synchronized(zookeeper){
            zookeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zookeeper.close();
    }

    public void watchTargetZnode() throws InterruptedException, KeeperException {
        Stat stat = zookeeper.exists(TARGET_ZNODE, this);

        if(stat == null){
            return;
        }

        byte[] data = zookeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zookeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("DATA : " + new String(data) + " children : " + children);
    }

    @Override
    public void process(WatchedEvent event) {
        switch(event.getType()){
            case None:
                //we successfully synchronized with Zookeeper server
                if(event.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("Successfully Connected to the Server");
                    //if we lost the connection to the zookeeper, wake.
                }else{
                    synchronized (zookeeper){
                        System.out.println("Disconnected from zookeeper event");
                        zookeeper.notifyAll();
                    }
                }
                break;

            case NodeDeleted:
                try {
                    reElectLeader();
                }catch (InterruptedException ex) {
                }catch (KeeperException e){
                }

            case NodeCreated:
                System.out.println(TARGET_ZNODE + " was created!");
                break;

            case NodeDataChanged:
                System.out.println(TARGET_ZNODE + " data changed!");
                break;

            case NodeChildrenChanged:
                System.out.println(TARGET_ZNODE + " children changed!");
                break;
        }

        try {
            watchTargetZnode();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
