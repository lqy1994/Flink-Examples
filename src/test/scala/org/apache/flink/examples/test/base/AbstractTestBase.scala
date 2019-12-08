package org.apache.flink.examples.test.base

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.junit.Rule
import org.junit.rules.TemporaryFolder

class AbstractTestBase extends Serializable {

  val temFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = temFolder

  def getStateBackend: StateBackend = {
    val dbPath = tempFolder.newFolder().getAbsolutePath
    val checkpointPath = tempFolder.newFolder().toURI.toString
    val backend = new RocksDBStateBackend(checkpointPath)
    backend.setDbStoragePath(dbPath)
    backend
  }

}
