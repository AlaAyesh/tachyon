/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker;

import java.io.Closeable;
import java.io.PrintStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * Defines how to interact with a server running the data protocol.
 */
public interface DataServer extends Closeable {

  class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

    public static ArrayList<DataServer> createDataServer(final InetSocketAddress dataAddress,
        final BlocksLocker blockLocker, TachyonConf conf) {
      String server = "";
      ArrayList<DataServer> workersDataServers = new ArrayList<DataServer>();
      try {
        Class<?>[] classes = new Class[] {InetSocketAddress.class, BlocksLocker.class, 
            TachyonConf.class};
        Object[] objects = new Object[] {dataAddress, blockLocker, conf};
        List<Class<DataServer>> servers = (List<Class<DataServer>>) (List) conf.getClasses(
            Constants.WORKER_DATA_SEVRER,Constants.WORKER_DATA_SERVER_CLASS);
        for (int i = 0; i < servers.size(); i ++) {
          server = servers.get(i).getName();
          workersDataServers.add(CommonUtils.createNewClassInstance(servers.get(i), 
              classes, objects));
        }
        return (workersDataServers);
      } catch (Exception e) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        e.printStackTrace(printStream);
        printStream.close();
        if (outputStream.toString().contains("Address already in use")) {
          LOG.error("Can't start two data servers that use the same transport. Failed to start {}",
              server);
          return (workersDataServers);
        } else {
          throw Throwables.propagate(e);
        }
      }
    }
  }

  public abstract int getPort();

  public abstract boolean isClosed();
}
