package simpledb.storage;

import simpledb.common.Type;
//import simpledb.myStorage.myTupleDesc;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        //return items.iterator();
        // Done
        return new Iterator<TDItem>() {
            private int idx = -1;

            @Override
            public boolean hasNext() {
                return idx+1<types.length;
            }
            @Override
            public TDItem next() {
                if (++idx == types.length) {
                    throw new NoSuchElementException();
                } else {
                    return new TDItem(types[idx], names[idx]);
                }
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException("unimplemented");
            }
        };
    }

    private static final long serialVersionUID = 1L;
    //linked field by TDItem
    private List<TDItem> items ;
    private final Type[] types;
    private final String[] names;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
//        if (typeAr.length == 0) {
//            throw new IllegalArgumentException("at least a thing");
//        }
//        if (typeAr.length != fieldAr.length) {
//            throw new IllegalArgumentException("length must be same");
//        }
//        this.items = new ArrayList<TDItem>(typeAr.length);
//        for (int i = 0;i<typeAr.length;i++){
//            this.items.add(new TDItem(typeAr[i],fieldAr[i]));
//        }
//        items = new ArrayList<TDItem>(typeAr.length);
//        for (int i = 0; i < typeAr.length; i++) {
//            items.add(new TDItem(typeAr[i], fieldAr[i]));
//        }
        // Done
        types = new Type[typeAr.length];
        for (int i=0; i<typeAr.length; i++) {
            types[i] = typeAr[i];
        }
        names = new String[typeAr.length];
        for (int i=0; i<fieldAr.length; i++) {
            names[i] = fieldAr[i];
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        //this(typeAr,new String[typeAr.length]);
        this(typeAr, new String[0]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        //return items.size();
        return types.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
//        if (i>=items.size()){
//            throw new NoSuchElementException();
//        }
//        return items.get(i).fieldName;
        // Done
        if (i<0 || i>=types.length) {
            throw new NoSuchElementException("Index Out of Range.");
        } else {
            return names[i];
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
//        if (i>=numFields()){
//            throw new NoSuchElementException();
//        }
//        return this.items.get(i).fieldType;
        // Done
        if (i<0 || i>=types.length) {
            throw new NoSuchElementException("Index Out of Range.");
        } else {
            return types[i];
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
//        for (int i =0;i<items.size();i++){
//            String fieldName = items.get(i).fieldName;
//            if (fieldName != null && fieldName.equals(name)){
//                return i;
//            }
//        }
//        throw new NoSuchElementException();
        // Done
        if (null != name) {
            for (int i=0; i<names.length; i++) {
                if (name.equals(names[i])) {
                    return i;
                }
            }
        }
        throw new NoSuchElementException("Unknown Field Name.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        //size of the tupleDesc
//        int size = 0;
//        synchronized (this) {
//            for (int i = 0; i < items.size(); i++) {
//                size += items.get(i).fieldType.getLen();
//            }
//        }
//        return size;
//        int size = 0;
//        for (int i = 0; i < items.size(); i++) {
//            size += items.get(i).fieldType.getLen();
//        }
//        return size;
        int size = 0;

        for (int i=0; i<types.length; i++) {
            size += types[i].getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
//        int mergeLen = td1.numFields() + td2.numFields();
//        Type[] typeAr = new Type[mergeLen];
//        String[] fieldAr = new String[mergeLen];
//        for (int i = 0;i< td1.numFields();i++){
//            typeAr[i] = td1.getFieldType(i);
//            fieldAr[i] = td1.getFieldName(i);
//        }
//        for (int i = td1.numFields();i< mergeLen;i++){
//            typeAr[i] = td2.getFieldType(i- td1.numFields());
//            fieldAr[i] = td2.getFieldName(i- td1.numFields());
//        }
//        return new TupleDesc(typeAr,fieldAr);
        /////////////////////////////////////////////////////////////////
//        int td1Len = td1.numFields(),
//                td2Len = td2.numFields(),
//                len = td1Len + td2Len;
//
//        Type[] typeAr = new Type[len];
//        String[] fieldAr = new String[len];
//
//        for (int i = 0; i < td1Len; i++) {
//            typeAr[i] = td1.items.get(i).fieldType;
//            fieldAr[i] = td1.items.get(i).fieldName;
//        }
//
//        for (int i = td1Len; i < len; i++) {
//            typeAr[i] = td2.items.get(i - td1.numFields()).fieldType;
//            fieldAr[i] = td2.items.get(i - td1.numFields()).fieldName;
//        }
//        return new TupleDesc(typeAr, fieldAr);
        /////////////////////////////////////////////////////////////
        // Done
        Type[] types = new Type[td1.types.length+td2.types.length];
        String[] names = new String[td1.names.length+td2.names.length];
        int idx = 0;

        for (int i=0; i<td1.types.length; i++) {
            types[idx] = td1.types[i];
            names[idx++] = td1.names[i];
        }
        for (int i=0; i<td2.types.length; i++) {
            types[idx] = td2.types[i];
            names[idx++] = td2.names[i];
        }
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
//        if (o ==null){
//            return items == null;
//        }
//        if (o.getClass()== TupleDesc.class && items.equals(((TupleDesc) o).items)){
//            return true;
//        }
//        return false;
//        return o == null ? items == null :
//                (o instanceof TupleDesc && items.equals(((TupleDesc) o).items));
        if (!(o instanceof TupleDesc)) {
            return false;
        } else {
            TupleDesc other = (TupleDesc)o;

            if (this.numFields() != other.numFields()) {
                return false;
            } else {
                int n = this.numFields();

                for (int i=0; i<n; i++) {
                    if (null == this.getFieldName(i)) {
                        if (null != other.getFieldName(i)) {
                            return false;
                        }
                    } else if (!this.getFieldName(i).equals(other.getFieldName(i))) {
                        return false;
                    } else if (this.getFieldType(i) != other.getFieldType(i)) { // Will this work ?
                        return false;
                    }
                }
                return true;
            }
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
//        String s = new String();
//        for (int i = 0;i<numFields()-1;i++){
//            s += items.get(i).toString()+", ";
//        }
//        s += items.get(numFields()-1).toString();
//        return s;
//        String str = new String();
//        for (int i = 0; i < numFields() - 1; i++) {
//            str += (items.get(i).toString() + ", ");
//        }
//        str += items.get(numFields()-1).toString();
//        return str;
        // Done
        StringBuilder sb = new StringBuilder();
        int n = this.numFields();

        sb.append(getFieldType(0));
        sb.append("("+this.getFieldName(0)+")");
        for (int i=1; i<n; i++) {
            sb.append(","+getFieldType(i));
            sb.append("("+this.getFieldName(i)+")");
        }
        return sb.toString();
    }

}
