package com.inmobi.databus;

import com.inmobi.databus.DatabusConfig.*;
import com.inmobi.databus.consume.*;
import com.inmobi.databus.distcp.*;
import com.inmobi.databus.purge.*;
import com.inmobi.databus.zookeeper.*;
import org.apache.log4j.*;

import java.util.*;

public class Databus {
  private static Logger LOG = Logger.getLogger(Databus.class);
  private DatabusConfig config;

  public Set<String> getClustersToProcess() {
    return clustersToProcess;
  }

  private final Set<String> clustersToProcess;
  private final List<AbstractCopier> copiers = new ArrayList<AbstractCopier>();


  public Databus(DatabusConfig config, Set<String> clustersToProcess) {
    this.config = config;
    this.clustersToProcess = clustersToProcess;
  }

  public DatabusConfig getConfig() {
    return config;
  }

  public void init() throws Exception {
    for (Cluster cluster : config.getClusters().values()) {
      if (!clustersToProcess.contains(cluster.getName())) {
        continue;
      }
      //Start LocalStreamConsumerService for this cluster if it's the source of any stream
      if (cluster.getSourceStreams().size() > 0) {
        copiers.add(new LocalStreamConsumerService(config, cluster));
      }

      List<Cluster> mergedStreamRemoteClusters = new ArrayList<Cluster>();
      List<Cluster> mirroredRemoteClusters = new ArrayList<Cluster>();
      for (ConsumeStream cStream : cluster.getConsumeStreams().values()) {
        //Start MergedStreamConsumerService instances for this cluster for each cluster
        //from where it has to fetch a partial stream and is hosting a primary stream
        //Start MirroredStreamConsumerService instances for this cluster for each cluster
        //from where it has to mirror mergedStreams

        for (String cName : config.getStreams().get(cStream.getName())
                .getSourceClusters()) {
          if (cStream.isPrimary())
            mergedStreamRemoteClusters.add(config.getClusters().get(cName));
        }
        if (!cStream.isPrimary())  {
          Cluster primaryCluster = config.getPrimaryClusterForConsumeStream(cStream.getName());
          if (primaryCluster != null)
            mirroredRemoteClusters.add(primaryCluster);
        }
      }


      for (Cluster remote : mergedStreamRemoteClusters) {
        copiers.add(new MergedStreamConsumerService(config, remote, cluster));
      }
      for (Cluster remote : mirroredRemoteClusters) {
        copiers.add(new MirrorStreamConsumerService(config, remote, cluster));
      }
    }

    //Start a DataPurgerService for this Cluster/Clusters to process
    Iterator it = clustersToProcess.iterator();
    while(it.hasNext()) {
      String  clusterName = (String) it.next();
      Cluster cluster =  config.getClusters().get(clusterName);
      LOG.info("Starting Purger for Cluster [" + clusterName + "]");
      //Start a purger per cluster
      copiers.add(new DataPurgerService(config, cluster));
    }
  }

  public synchronized void start() {
    for (AbstractCopier copier : copiers) {
      copier.start();
    }
  }

  public synchronized void stop() {
    for (AbstractCopier copier : copiers) {
      copier.stop();
    }
    for (AbstractCopier copier : copiers) {
      copier.join();
    }
  }

  public void startDatabusWork() throws Exception{
    startDatabus();
    //Block this method to avoid losing leadership
    //of current work
    for (AbstractCopier copier : copiers) {
      copier.join();
    }
  }

  private void startDatabus() throws Exception{
    init();
    start();
  }

  public static void main(String[] args) throws Exception {
    try {
      if (args.length != 1 && args.length != 2 ) {
        LOG.warn("Usage: com.inmobi.databus.Databus <clustersToProcess> <configFile>");
        return;
      }
      String clustersStr = args[0].trim();
      String[] clusters = clustersStr.split(",");

      String databusconfigFile = null;
      if (args.length == 2) {
        databusconfigFile = args[1].trim();
      }
      DatabusConfigParser configParser =
              new DatabusConfigParser(databusconfigFile);
      Map<String, Cluster> clusterMap = configParser.getClusterMap();
      DatabusConfig config = new DatabusConfig(configParser.getRetentionInDays(), configParser.getRootDir(),
              configParser.getZkConnectString(),
              configParser.getStreamMap(), clusterMap);

      Set<String> clustersToProcess = new HashSet<String>();
      if (clusters.length == 1 && "ALL".equalsIgnoreCase(clusters[0])) {
        for (Cluster c : config.getClusters().values()) {
          clustersToProcess.add(c.getName());
        }
      } else {
        for (String c : clusters) {
          if (config.getClusters().get(c) == null) {
            LOG.warn("Cluster name is not found in the config - " + c);
            return;
          }
          clustersToProcess.add(c);
        }
      }
      Databus databus = new Databus(config, clustersToProcess);
      if (clustersToProcess.size() == 1 &&
              !"ALL".equalsIgnoreCase(clusters[0])) {
        //Elect a leader and then start
        LOG.info("Starting CuratorLeaderManager for eleader election ");
        CuratorLeaderManager curatorLeaderManager =  new CuratorLeaderManager(databus);
        curatorLeaderManager.becomeLeader();
      }
      else {
        //Running in simulated mode don't use ZK
        databus.startDatabus();
      }

    }
    catch (Exception e) {
      LOG.warn(e.getMessage());
      LOG.warn(e);
      throw new Exception(e);
    }
  }

}
