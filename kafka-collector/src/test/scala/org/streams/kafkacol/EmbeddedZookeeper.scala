package org.streams.kafkacol

import org.apache.zookeeper.server.NIOServerCnxn
import org.apache.zookeeper.server.ZooKeeperServer
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import java.io.File
import java.net.InetSocketAddress

class EmbeddedZookeeper(dataLogDir: File, dataDir: File) {

  val zkServer = new ZooKeeperServer();
  val ftxn = new FileTxnSnapLog(dataLogDir, dataDir);

  zkServer.setTxnLogFactory(ftxn);
  zkServer.setTickTime(200);
  zkServer.setMinSessionTimeout(1000);
  zkServer.setMaxSessionTimeout(1000);
  //val cnxnFactory = new NIOServerCnxn.Factory(new InetSocketAddress(2181),
  //  100);
  //cnxnFactory.startup(zkServer);

  
  //  cnxnFactory.configure(new InetSocketAddress(2181),
  //    1000);
  //  cnxnFactory.startup(zkServer);

  while (!zkServer.isRunning()) {
    println("Waiting for zookeeper startup")
    Thread.sleep(100)
  }

  println("ZK Started on port: 2181 : isRunning: " + zkServer.isRunning() + " " + zkServer.getState())

  def shutdown() {
    zkServer.shutdown()
  }

}
