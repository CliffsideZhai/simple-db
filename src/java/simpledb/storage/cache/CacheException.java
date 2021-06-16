package simpledb.storage.cache;

import simpledb.common.DbException;

public class CacheException extends DbException {
    public CacheException(String s) {
        super(s);
    }

    private static final long serialVersionUID = 1L;
}
