package org.lockss.laaws.indexdb.indexstore;

import java.util.List;
import java.net.MalformedURLException;
import java.net.URL;

public interface IndexStoreInterface {
	// IndexStore operations
	WarcIndexInterface indexWarc(URL url);
	void removeWarc(WarcIndexInterface index);
	
	// LOCKSS specific indexing operations
	List<String> getAllAuids();
	List<URL> getURLsWithAuid(String auid);
	List<String> getAuidsWithURL(URL url);
	default List<String> getAuidsWithURL(String url) throws MalformedURLException {
		return this.getAuidsWithURL(new URL(url));
	}

	// OpenWayback specific indexing operations
	WarcRecordIndex getWarcRecordIndex(URL url);
	WarcRecordIndex getWarcRecordIndex(URL url, Integer timestamp);
	List<WarcRecordIndex> getWarcRecordIndexes(URL url);

}
