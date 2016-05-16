package org.lockss.laaws.indexdb.indexer;

import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;

import org.archive.util.ArchiveUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

public class WarcIndexer {
    public static void main(String[] args) {
        try {
            WARCReader reader = WARCReaderFactory.get("/home/daniel/warcs/test-small.warc");
            Iterator wolf = reader.iterator();
            while (wolf.hasNext()) {
                ArchiveRecord pup = (ArchiveRecord)wolf.next();
                System.out.println("----------------");
                //pup.dump();
                //System.out.println(pup.getHeader().getHeaderFields());
                try {
                    System.out.println(pup.hasContentHeaders());
                    System.out.println(pup.getHeader().getUrl());
                    System.out.println(ArchiveUtils.getDate(pup.getHeader().getDate()).getTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
