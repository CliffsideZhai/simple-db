package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {


    private static final long serialVersionUID = 1L;

    private final PageId pid;
    private final int tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleno = tupleno;
        // some code goes here
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return this.tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return this.pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        if (o != null && o.getClass() == RecordId.class) {
            return this.pid.equals(((RecordId) o).getPageId())
                    && this.tupleno == ((RecordId) o).getTupleNumber();
        }
        return false;
//        if (o != null && o instanceof RecordId) {
//            RecordId rid = (RecordId) o;
//            return rid.tupleno == this.tupleno
//                    && rid.pid.equals(this.pid);
//        }
//        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return String.valueOf(pid.hashCode())
                .concat(String.valueOf(tupleno))
                .hashCode();
    }

}
