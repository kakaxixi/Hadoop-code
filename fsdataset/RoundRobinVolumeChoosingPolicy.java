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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**
 * Choose volumes in round-robin order.
 */
//V只是一个泛型参数名称，无实际意义，需要在该方法被调用时才能知道V到底是什么类型
//V参数的实参必须是FsVolumeSpi的子类
public class RoundRobinVolumeChoosingPolicy<V extends FsVolumeSpi>
    implements VolumeChoosingPolicy<V> {
  public static final Log LOG = LogFactory.getLog(RoundRobinVolumeChoosingPolicy.class);
//在哪个类打日志添加的对象
  private int curVolume = 0;

private double cpuUsage;
private double memUsage = 0.0;
private double ioUsage = 0.0;

  @Override
  public synchronized V chooseVolume(final List<V> volumes, long blockSize)//List代表java列表
      throws IOException {
	 
    if(volumes.size() < 1) {//.size()是List自带函数不是编写的
      throw new DiskOutOfSpaceException("No more available volumes");
    }
    
    // since volumes could've been removed because of the failure
    // make sure we are not out of bounds
    if(curVolume >= volumes.size()) {
      curVolume = 0;
    }
    
    int startVolume = curVolume;
    long maxAvailable = 0;
    
    //------------------wanglai
	//double cpuUsage = 0.0;
	//double memUsage = 0.0;
	//double ioUsage = 0.0;
	BufferedReader mpstatReader = null;

	String mpstatLine;
	String[] mpstatChunkedLine;

	double idle;

	try {
		Runtime runtime = Runtime.getRuntime();
		Process mpstatProcess = runtime.exec("iostat -x");

		mpstatReader = new BufferedReader(new InputStreamReader(
				mpstatProcess.getInputStream()));

		mpstatReader.readLine();
		mpstatReader.readLine();
		mpstatReader.readLine();

		mpstatLine = mpstatReader.readLine();
		if (mpstatLine == null) {
			throw new Exception("iostat didn't work well");
		} else {
			mpstatChunkedLine = mpstatLine.split("\\s+");
			idle = Double
					.parseDouble(mpstatChunkedLine[mpstatChunkedLine.length - 1]);
			this.cpuUsage = 100 - idle;
		}
		LOG.info("--wanglairoundrobin--cpuusage:"+this.cpuUsage);
		mpstatReader.readLine();
		mpstatReader.readLine();
		mpstatLine = mpstatReader.readLine();
		if (mpstatLine == null) {
			throw new Exception("iostat didn't work well");
		} else {
			mpstatChunkedLine = mpstatLine.split("\\s+");
			this.ioUsage = Double
					.parseDouble(mpstatChunkedLine[mpstatChunkedLine.length - 1]);
			LOG.info("--wanglairoundrobin--iousage:"+this.ioUsage);
		}
	} catch (Exception e) {
		try {
			throw e;
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} // It's not desirable to handle the exception here
	} finally {
		if (mpstatReader != null)
			try {
				mpstatReader.close();
			} catch (IOException e) {
				// Do nothing
			}
	}
	//----------------------------------------
    while (true) {
      final V volume = volumes.get(curVolume);//.get()是List自带函数不是编写的
      curVolume = (curVolume + 1) % volumes.size();
      long availableVolumeSize = volume.getAvailable();
      if (availableVolumeSize > blockSize) {
	LOG.info("--wanglairoundrobin");
        return volume;
      }
      
      if (availableVolumeSize > maxAvailable) {
        maxAvailable = availableVolumeSize;
      }
      
      if (curVolume == startVolume) {
        throw new DiskOutOfSpaceException("Out of space: "
            + "The volume with the most available space (=" + maxAvailable
            + " B) is less than the block size (=" + blockSize + " B).");
      }
    }
  }
}
