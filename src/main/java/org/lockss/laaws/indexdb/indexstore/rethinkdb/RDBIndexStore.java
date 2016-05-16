package org.lockss.laaws.indexdb.indexstore.rethinkdb;

import java.net.URL;
import java.util.List;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import org.lockss.laaws.indexdb.indexstore.IndexStoreInterface;
import org.lockss.laaws.indexdb.indexstore.WarcIndexInterface;
import org.lockss.laaws.indexdb.indexstore.WarcRecordIndex;

public class RDBIndexStore implements IndexStoreInterface {
	private static final String INDEXDB = "lockss";
	private static final String INDEX   = "urlindex";
	private static final String AUID_COLUMN = "auid";
	private static final String URL_COLUMN = "url";
	private static final String TIMESTAMP_COLUMN = "timestamp";
	private static final String WARCRECORD_INDEX = "warcrecord_index";
    private static final String WARC_COLUMN = "warc";

	private static final RethinkDB r = RethinkDB.r;
    private final Connection conn;

	public RDBIndexStore() {
		this.conn = r.connection().connect();
		//conn.use(INDEXDB);
		this.initialize();
	}

	// This is purely for early development convenience; a race condition will
	// occur if there are multiple threads calling this. Create the production
	// database and tables some other way.
	private void initialize() {
		// Create the database and table
		createDB(INDEXDB);
		createTable(INDEX);

		// Create special indexes on the index table
        createIndex(WARC_COLUMN, r.db(INDEXDB).table(INDEX).indexCreate(WARC_COLUMN).optArg("multi", true));
		createIndex(AUID_COLUMN, r.db(INDEXDB).table(INDEX).indexCreate(AUID_COLUMN).optArg("multi", true));
        createIndex(URL_COLUMN, r.db(INDEXDB).table(INDEX).indexCreate(URL_COLUMN));
		createIndex(WARCRECORD_INDEX, r.db(INDEXDB).table(INDEX).indexCreate(WARCRECORD_INDEX,
			row -> r.array(
				row.g(URL_COLUMN),
				row.g(TIMESTAMP_COLUMN)
			)
		));

		r.db(INDEXDB).table(INDEX).indexWait(WARC_COLUMN);
		r.db(INDEXDB).table(INDEX).indexWait(AUID_COLUMN);
		r.db(INDEXDB).table(INDEX).indexWait(URL_COLUMN);
		r.db(INDEXDB).table(INDEX).indexWait(WARCRECORD_INDEX);
	}

    private void createIndex(String indexName, ReqlAst index) {
        System.out.println(index.getClass());
        r.db(INDEXDB).table(INDEX).indexList().contains(indexName).do_(
            indexExists -> r.branch(
                indexExists, null, index
            )
        ).run(conn);
    }

	private void createDB(String db) {
		r.dbList().contains(db).do_(
			dbExists -> r.branch(
				dbExists,
				null,
				r.dbCreate(db)
			)
		).run(conn);
	}

	private void createTable(String table) {
		createTable(INDEXDB, table);
	}

	private void createTable(String db, String table) {
		r.tableCreate(table);
		r.db(db).tableList().contains(table).do_(
			tableExists -> r.branch(
				tableExists,
				null,
				r.db(db).tableCreate(table)
			)
		).run(conn);
	}

	private static MapObject createURLRow(URL url, String auid, Integer timestamp, String warc, Integer offset) {
		return r.hashMap("url", url.toString())
		.with("timestamp", timestamp)
		.with("auid", auid)
		.with("warc", warc)
		.with("offset", offset);
	}

	public void addURL(URL url, String auid, Integer timestamp, String warc, Integer offset) {
		r.db(INDEXDB).table(INDEX).insert(createURLRow(url, auid, timestamp, warc, offset)).run(conn);
	}

	public void printTable(String table) {
		Cursor<?> cursor = r.db(INDEXDB).table(table).run(conn);
		for (Object doc : cursor) {
			System.out.println(doc);
		}
	}

    @Override
    public WarcIndexInterface indexWarc(URL url) {
        return new RDBWarcIndex();
    }

    @Override
    public void removeWarc(WarcIndexInterface index) {
        r.db(INDEXDB).table(INDEX).getAll(index.getId()).optArg("index", WARC_COLUMN).delete().run(conn);
    }

    @Override
	public List<String> getAllAuids() {
		// TODO: The use of concatMap() implies the AUID column is an array; if
		// we plan to use a flat schema, this should be updated.
		return r.db(INDEXDB).table(INDEX)
                .pluck(AUID_COLUMN)
                //.concatMap(auid -> r.array(auid.g(AUID_COLUMN)))
                .distinct()
                .run(conn);
	}

    @Override
    public List<URL> getURLsWithAuid(String auid) {
		return r.db(INDEXDB).table(INDEX)
                .getAll(auid).optArg("index", AUID_COLUMN)
                .pluck(URL_COLUMN)
                .concatMap(url -> r.array(url.g(URL_COLUMN)))
                .distinct().run(conn);
    }

    @Override
    public List<String> getAuidsWithURL(URL url) {
		return r.db(INDEXDB).table(INDEX)
                .getAll(url.toString()).optArg("index", URL_COLUMN)
                .pluck(AUID_COLUMN)
                .concatMap(auid -> r.array(auid.g(AUID_COLUMN)))
                .distinct().run(conn);
    }

    @Override
    // Returns the most recent version of a URL
    public WarcRecordIndex getWarcRecordIndex(URL url) {
        return new WarcRecordIndex(r.db(INDEXDB).table(INDEX)
                .getAll(url.toString()).optArg("index", URL_COLUMN)
                .max(row -> row.g(TIMESTAMP_COLUMN))
                .run(conn));
    }

    @Override
    // We allow for the possibility of a non-unique secondary index and return the first document found: This
    // relies on the fact that the observed content at a given URL and timestamp should be the same universally;
    // that may turn out to be a poor assumption in practice.
    public WarcRecordIndex getWarcRecordIndex(URL url, Integer timestamp) {
        return new WarcRecordIndex(r.db(INDEXDB).table(INDEX)
                .getAll(r.array(url.toString(), timestamp))
                .optArg("index", WARCRECORD_INDEX)
                .nth(0)
                .run(conn));
    }

    @Override
    // Returns a set of WarcRecordIndex objects for a given URL, spanning timestamp and WARC file
    public List<WarcRecordIndex> getWarcRecordIndexes(URL url) {
        return null;
    }

	public void close() {
		this.conn.close();
	}
}
