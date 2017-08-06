/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter;

import org.apache.calcite.schema.SchemaPlus;

public class TestCalciteIndexScan extends TestCalciteBase {

    SchemaPlus m_schemaPlus;

    @Override
    public void setUp() throws Exception {
        setupSchema(TestCalciteIndexScan.class.getResource("testcalcite-scan-ddl.sql"),
                "testcalcitescan", false);
        m_schemaPlus = schemaPlusFromDDL();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testIndexScanNoFilter() throws Exception {
        String sql;
        // Calcite produces a SeqScan here
        sql = "select * from RI1";
        comparePlans(sql);
    }

    public void testIndexScanNoFilter10() throws Exception {
        String sql;
        // Calcite produces a SeqScan here
        sql = "select i, bi from RI1";
        comparePlans(sql);
    }

    public void testIndexScan() throws Exception {
        String sql;
        sql = "select * from RI1 where i=5";
        comparePlans(sql);
    }


}
