/*

Copyright (c) 2016, Board of Trustees of Leland Stanford Jr. University,
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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
