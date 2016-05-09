/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_REFERENCE_COUNT_VOLUME_CHOOSING_POLICY_REFERENCE_COUNT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_REFERENCE_COUNT_VOLUME_CHOOSING_POLICY_REFERENCE_COUNT_THRESHOLD_KEY;
/**
 * Choose volumes by fsVolume reference count
 */
public class ReferenceCountVolumeChoosingPolicy<V extends FsVolumeSpi>
    implements VolumeChoosingPolicy<V>, Configurable{
  public static final Log LOG = LogFactory
      .getLog(ReferenceCountVolumeChoosingPolicy.class);

  private int referenceThreshold =
      DFS_DATANODE_REFERENCE_COUNT_VOLUME_CHOOSING_POLICY_REFERENCE_COUNT_THRESHOLD_DEFAULT;
  private final VolumeChoosingPolicy<V> roundRobinPolicyLowReferences =
      new RoundRobinVolumeChoosingPolicy<V>();
  private final VolumeChoosingPolicy<V> roundRobinPolicyHighReferences =
      new RoundRobinVolumeChoosingPolicy<V>();

  @Override
  public synchronized void setConf(Configuration conf) {
    referenceThreshold =
        conf.getInt(
            DFS_DATANODE_REFERENCE_COUNT_VOLUME_CHOOSING_POLICY_REFERENCE_COUNT_THRESHOLD_KEY,
            DFS_DATANODE_REFERENCE_COUNT_VOLUME_CHOOSING_POLICY_REFERENCE_COUNT_THRESHOLD_DEFAULT);
    LOG.info("--wanglai-referencecount--setconf");
  }

  @Override
  public synchronized Configuration getConf() {
    return null;
  }

  @Override
  public synchronized V chooseVolume(final List<V> volumes, long blockSize)
      throws IOException {

    if (volumes.size() < 1) {
      throw new DiskOutOfSpaceException("No more available volumes");
    }
  //获取当前磁盘中被引用次数最少的1块盘 
    V volume = null;

    int minReferenceCount = getMinReferenceCountOfVolumes(volumes);
  //根据最少引用次数以及引用计数临界值得到低引用计数磁盘列表  
    List<V> lowReferencesVolumes =
        getLowReferencesCountVolume(volumes, minReferenceCount);
  //根据最少引用次数以及引用计数临界值得到高引用计数磁盘列表  
    List<V> highReferencesVolumes =
        getHighReferencesCountVolume(volumes, minReferenceCount);

    //判断低引用磁盘列表中是否存在满足要求块大小的磁盘,如果有优选从低磁盘中进行轮询磁盘的选择 
    if (isExistVolumeHasFreeSpaceForBlock(lowReferencesVolumes, blockSize)) {
      volume =
          roundRobinPolicyLowReferences.chooseVolume(lowReferencesVolumes,
              blockSize);
    } else {
    	//如果低磁盘块中没有可用空间的块,则再从高引用计数的磁盘列表中进行磁盘的选择
      volume =
          roundRobinPolicyHighReferences.chooseVolume(highReferencesVolumes,
              blockSize);
    }
    LOG.info("--wanglai-referencecount--choosevolume");
    return volume;
  }

  private List<V> getHighReferencesCountVolume(final List<V> volumes,
      int minReferenceCount) {
    List<V> newVolumes = new ArrayList<V>();

    for (V v : volumes) {
      if (v.getReferenceCount() > (minReferenceCount + referenceThreshold)) {
        newVolumes.add(v);
      }
    }

    return newVolumes;
  }

  private List<V> getLowReferencesCountVolume(final List<V> volumes,
      int minReferenceCount) {
    List<V> newVolumes = new ArrayList<V>();

    for (V v : volumes) {
      if (v.getReferenceCount() <= (minReferenceCount + referenceThreshold)) {
        newVolumes.add(v);
      }
    }

    return newVolumes;
  }

  private int getMinReferenceCountOfVolumes(final List<V> volumes) {
    int minReferenceCount = Integer.MAX_VALUE;

    int curReferenceCount;
    for (V v : volumes) {
      curReferenceCount = v.getReferenceCount();
      if (curReferenceCount < minReferenceCount) {
        minReferenceCount = curReferenceCount;
      }
    }

    return minReferenceCount;
  }

  private boolean isExistVolumeHasFreeSpaceForBlock(final List<V> volumes,
      long blockSize) throws IOException {
    boolean isExist = false;

    for (V v : volumes) {
      if (v.getAvailable() >= blockSize) {
        isExist = true;
        break;
      }
    }

    return isExist;
  }
}
