package org.lockss.laaws.indexdb.indexstore;

import org.lockss.laaws.indexdb.indexstore.WarcIndexInterface;

import java.util.Map;

public class WarcRecordIndex implements WarcIndexInterface {

    public WarcRecordIndex(Map obj) {
        System.out.println(obj);
    }

    @Override
    public String getId() {
        return null;
    }
}
