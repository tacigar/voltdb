/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.NullCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestRejoinWithSnapshot extends JUnit4LocalClusterTest {

    VoltLogger m_logger = new VoltLogger("TEST");

    @After public void tearDown() throws Exception {
        System.gc();
        System.runFinalization();
    }

    private void checkView(Client client, long count) throws Exception {
        VoltTable viewContent = client.callProcedure("@AdHoc", "select * from v order by 1;").getResults()[0];
        for (long i = 1; i <= 5000; i++) {
            assertTrue(viewContent.advanceRow());
            assertEquals(i, viewContent.getLong(0));
            assertEquals(count, viewContent.getLong(1));
        }
    }

    /**
     * https://issues.voltdb.com/browse/ENG-15174
     */
    @Test public void testRejoinAndSnapshot() throws Exception {
        String ddl = "create table t (i bigint not null, j bigint not null);\n" +
                     "partition table t on column i;\n" +
                     "create view v (i, counti) AS select i, count(*) from t group by i;";
        // Reset the VoltFile prefix that may have been set by previous tests in this suite
        org.voltdb.utils.VoltFile.resetSubrootForThisProcess();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(ddl);
        int sitesPerHost = 4, hostCount = 3, kFactor = 2;
        LocalCluster cluster = new LocalCluster("rejoin.jar", sitesPerHost,
                hostCount, kFactor, BackendTarget.NATIVE_EE_JNI);
        cluster.overrideAnyRequestForValgrind();
        assertTrue(cluster.compile(builder));
        MiscUtils.copyFile(builder.getPathToDeployment(), VoltConfiguration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);
        cluster.startUp();

        Client client = cluster.createClient(new ClientConfig());
        NullCallback callback = new NullCallback();
        for (int i = 1; i <= 5000; i++) {
            client.callProcedure(callback, "T.insert", i, 5001 - i);
        }
        client.drain();
        checkView(client, 1);

        for (int i = 1; i <= 4; i++) {
            m_logger.info("Taking snapshot #" + i);
            client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"),
                    "testnonce" + i, (byte) 1);
            m_logger.info("Restoring from snapshot #" + i);
            client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"),
                    "testnonce" + i);
            client.close();
            int nodeToStop = (i - 1) % hostCount;
            m_logger.info("Stop node " + nodeToStop);
            cluster.killSingleHost(nodeToStop);
            Thread.sleep(1000);
            m_logger.info("Bring back node " + nodeToStop);
            cluster.recoverOne(nodeToStop, 1, "");
            client = cluster.createClient(new ClientConfig());
            checkView(client, 1 << i);
        }
    }
}
