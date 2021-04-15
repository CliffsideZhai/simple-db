//package simpledb.myStorage;
//
////import com.sun.org.apache.xpath.internal.operations.String;
//import simpledb.common.Type;
//
//import javax.print.DocFlavor;
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.IdentityHashMap;
//import java.util.Iterator;
//import java.util.NoSuchElementException;
//
///**
// * TupleDesc describes the schema of a tuple.
// */
//public class myTupleDesc implements Serializable {
//
//    /**
//     * A help class to facilitate organizing the information of each field
//     *
//     * */
//    public static class TDItem implements Serializable {
//
//        private static final long serialVersionUID = 1L;
//
//        /**
//         * The type of the field
//         * */
//        public final Type fieldType;
//
//        /**
//         * The name of the field
//         * */
//        public final String fieldName;
//
//        public TDItem(Type t, String n) {
//            this.fieldName = n;
//            this.fieldType = t;
//        }
//
//        public String toString() {
//            return fieldName + "(" + fieldType + ")";
//        }
//
//    }
//
//    private static final long serialVersionUID = 1L;
//
//    /**
//     *
//     * @return get arraylist of items
//     */
//    public ArrayList<TDItem> getItems() {
//        return items;
//    }
//
//    //linked field by TDItem
//    private ArrayList<TDItem> items ;
//    //size of the tupleDesc
//    private int size;
//
//    /**
//     * @return
//     *        An iterator which iterates over all the field TDItems
//     *        that are included in this TupleDesc
//     * */
//    public Iterator<TDItem> iterator() {
//        // some code goes here
//        // return a iterator of arraylist
//        return items.iterator();
//    }
//
//    /**
//     * Create a new TupleDesc with typeAr.length fields with fields of the
//     * specified types, with associated named fields.
//     *
//     * @param typeAr
//     *            array specifying the number of and types of fields in this
//     *            TupleDesc. It must contain at least one entry.
//     * @param fieldAr
//     *            array specifying the names of the fields. Note that names may
//     *            be null.
//     */
//    public myTupleDesc(Type[] typeAr, String[] fieldAr) {
//        // some code goes here
//        this.items = new ArrayList<TDItem>(typeAr.length);
//        Iterator<TDItem> iterator = items.iterator();
//        for (int i = 0;i<typeAr.length;i++){
//            this.items.add(new TDItem(typeAr[i],fieldAr[i]));
//        }
//    }
//
//    /**
//     * Constructor. Create a new tuple desc with typeAr.length fields with
//     * fields of the specified types, with anonymous (unnamed) fields.
//     *
//     * @param typeAr
//     *            array specifying the number of and types of fields in this
//     *            TupleDesc. It must contain at least one entry.
//     */
//    public myTupleDesc(Type[] typeAr) {
//        // some code goes here
//        this(typeAr,new String[typeAr.length]);
//    }
//
//    /**
//     * @return the number of fields in this TupleDesc
//     */
//    public int numFields() {
//        // some code goes here
//        return items.size();
//    }
//
//    /**
//     * Gets the (possibly null) field name of the ith field of this TupleDesc.
//     *
//     * @param i
//     *            index of the field name to return. It must be a valid index.
//     * @return the name of the ith field
//     * @throws NoSuchElementException
//     *             if i is not a valid field reference.
//     */
//    public String getFieldName(int i) throws NoSuchElementException {
//        // some code goes here
//        if (i>items.size()){
//            throw new NoSuchElementException();
//        }
//        return this.items.get(i).fieldName;
//    }
//
//    /**
//     * Gets the type of the ith field of this TupleDesc.
//     *
//     * @param i
//     *            The index of the field to get the type of. It must be a valid
//     *            index.
//     * @return the type of the ith field
//     * @throws NoSuchElementException
//     *             if i is not a valid field reference.
//     */
//    public Type getFieldType(int i) throws NoSuchElementException {
//        // some code goes here
//        if (i>items.size()){
//            throw new NoSuchElementException();
//        }
//        return items.get(i).fieldType;
//    }
//
//    /**
//     * Find the index of the field with a given name.
//     *
//     * @param name
//     *            name of the field.
//     * @return the index of the field that is first to have the given name.
//     * @throws NoSuchElementException
//     *             if no field with a matching name is found.
//     */
//    public int fieldNameToIndex(String name) throws NoSuchElementException {
//        // some code goes here
//        for (int i =0;i<items.size();i++){
//            TDItem tdItem = items.get(i);
//            if (tdItem != null && tdItem.equals(name)){
//                return i;
//            }
//        }
//        throw new NoSuchElementException();
//    }
//
//    /**
//     * This is the size of tupleDesc i,g how long the schema
//     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
//     *         Note that tuples from a given TupleDesc are of a fixed size.
//     */
//    public int getSize() {
//        // some code goes here
//        synchronized (this) {
//            for (int i = 0; i < items.size(); i++) {
//                this.size = items.get(i).fieldType.getLen();
//            }
//        }
//        return this.size;
//    }
//
//    /**
//     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
//     * with the first td1.numFields coming from td1 and the remaining from td2.
//     *
//     * @param td1
//     *            The TupleDesc with the first fields of the new TupleDesc
//     * @param td2
//     *            The TupleDesc with the last fields of the TupleDesc
//     * @return the new TupleDesc
//     */
//    public static myTupleDesc merge(myTupleDesc td1, myTupleDesc td2) {
//        // some code goes here
//        int mergeLen = td1.numFields() + td2.numFields();
//        Type[] typeAr = new Type[mergeLen];
//        String[] fieldAr = new String[mergeLen];
//        for (int i = 0;i< td1.numFields();i++){
//            typeAr[i] = td1.getFieldType(i);
//            fieldAr[i] = td1.getFieldName(i);
//        }
//        for (int i = td1.size;i< mergeLen;i++){
//            typeAr[i] = td2.getFieldType(i);
//            fieldAr[i] = td2.getFieldName(i);
//        }
//        return new myTupleDesc(typeAr,fieldAr);
//    }
//
//    /**
//     * Compares the specified object with this TupleDesc for equality. Two
//     * TupleDescs are considered equal if they have the same number of items
//     * and if the i-th type in this TupleDesc is equal to the i-th type in o
//     * for every i.
//     *
//     * @param o
//     *            the Object to be compared for equality with this TupleDesc.
//     * @return true if the object is equal to this TupleDesc.
//     */
//
//    public boolean equals(Object o) {
//        // some code goes here
//        if (o ==null){
//            return items == null;
//        }
//        if (o instanceof myTupleDesc &&((myTupleDesc) o).getItems().equals(this.items)){
//            return true;
//        }
//        return false;
//    }
//
//    public int hashCode() {
//        // If you want to use TupleDesc as keys for HashMap, implement this so
//        // that equal objects have equals hashCode() results
//
//        throw new UnsupportedOperationException("unimplemented");
//    }
//
//    /**
//     * Returns a String describing this descriptor. It should be of the form
//     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
//     * the exact format does not matter.
//     *
//     * @return String describing this descriptor.
//     */
//    public String toString() {
//        // some code goes here
//        String s = new String();
//        for (int i = 0;i<numFields()-1;i++){
//            s += items.get(i).toString()+", ";
//        }
//        s += items.get(numFields()).toString();
//        return s;
//    }
//}