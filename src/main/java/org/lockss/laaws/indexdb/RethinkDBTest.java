/*
Copyright (c) 2000-2016 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of Stanford University shall not
be used in advertising or otherwise to promote the sale, use or other dealings
in this Software without prior written authorization from Stanford University.
*/

package org.lockss.laaws.indexdb;

import org.lockss.laaws.indexdb.indexstore.rethinkdb.RDBIndexStore;

import java.net.URL;

public class RethinkDBTest {
	public static void main(String[] args) {
		RDBIndexStore ris = new RDBIndexStore();
        try {
            ris.addURL(new URL("http://whywolf.com/url1"), "auid1", 0, "warc1", 0);
            ris.addURL(new URL("http://whywolf.com/url2"), "auid1", 0, "warc1", 0);
            ris.addURL(new URL("http://whywolf.com/url3"), "auid2", 0, "warc1", 0);
            ris.addURL(new URL("http://whywolf.com/url4"), "auid2", 0, "warc1", 0);
            ris.addURL(new URL("http://whywolf.com/url4"), "auid2", 9, "warc1", 10);
            ris.addURL(new URL("http://whywolf.com/url4"), "auid2", 14, "warc1", 17);
            ris.addURL(new URL("http://whywolf.com/url4"), "auid2", 10, "warc1", 7);

            System.out.println("WOLF: " + ris.getAuidsWithURL(new URL("http://whywolf.com/url4")));
            System.out.println("WOOF: " + ris.getWarcRecordIndex(new URL("http://whywolf.com/url4" ), 9));
        } catch (Exception e) {
            System.out.println(e);
        }

        ris.printTable("urlindex");
        System.out.println(ris.getAllAuids());
        System.out.println(ris.getURLsWithAuid("auid1"));
        ris.close();

	}
}