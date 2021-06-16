package simpledb.storage.cache;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 使用 lru 最近最少使用算法来实现buffer pool的缓存换入换出
 * @param <K>
 * @param <V>
 */
public class LruCache<K,V>{

    protected class Node{
        Node front;
        Node next;
        K key;
        V value;

        public Node(K key,V v) {
            this.key = key;
            value = v;
        }
    }
    /**
     * 当前缓存
     */
    protected ConcurrentHashMap<K,Node> cacheEntries;

    /**
     * 允许缓存
     */
    protected int capacity;

    protected Node head;

    protected Node tail;

    public LruCache(int capacity) {
        this.capacity = capacity;
        cacheEntries =new ConcurrentHashMap<>(capacity);
        head= new Node(null,null);
    }

    /**
     * 删除结点
     * @param ruNode the recently used Node
     */
    protected void unlink(Node ruNode) {
        //如果是最后一个结点
        if (ruNode.next == null) {
            ruNode.front.next = null;
        } else {
            ruNode.front.next=ruNode.next;
            ruNode.next.front=ruNode.front;
        }
    }

    /**
     * 讲最近使用的节点，添加到头结点
     * @param ruNode 最近使用的节点
     */
    protected void linkFirst(Node ruNode){
        Node oldHead = this.head.next;
        this.head.next = ruNode;
        ruNode.next = oldHead;
        ruNode.front = this.head;
        if (oldHead == null){
            tail = ruNode;
        }else {
            oldHead.front = ruNode;
        }
    }
    /**
     * 删除链表的最后一个元素
     * @return  返回被删除的元素
     */
    protected K removeTail() {
        K element=tail.key;
        Node newTail = tail.front;
        tail.front=null;
        newTail.next=null;
        tail=newTail;
        return element;
    }

    /**
     *
     * @param key
     * @param value
     * @return     被删除出缓存的条目，如果没有，返回null
     */
    public V put(K key, V value) throws CacheException {
        if (key == null | value == null) {//不允许插入null值
            throw new IllegalArgumentException();
        }
        if (isCached(key)) {
            //该结点存在于cache中，则更新其值，然后调整最近使用的条目，返回null(因为没有被删除的条目)
            Node ruNode = cacheEntries.get(key);
            ruNode.value=value;
            // TODO : 讲之前的节点断开
            unlink(ruNode);
            // TODO : 加入到头
            linkFirst(ruNode);
            return null;
        } else  {
            //不存在的话先判断是否已经达到容量，是的话要先删除尾结点最后将其返回
            //还没有的话只需要新建结点，然后插入到表头，返回null
            V removed=null;
            if (cacheEntries.size() == capacity) {
                K removedKey=removeTail();
                removed = cacheEntries.remove(removedKey).value;
            }
            Node ruNode = new Node(key, value);
            linkFirst(ruNode);
            cacheEntries.put(key, ruNode);
            return removed;
        }
    }
    /**
     *
     * @param key
     * @return  返回存在与缓存中的条目，不存在则返回null
     */
    public V get(K key) {
        if (isCached(key)) {
            //调整最近使用的条目
            Node ruNode = cacheEntries.get(key);
            unlink(ruNode);
            linkFirst(ruNode);
            return ruNode.value;
        }
        return null;
    }
    public boolean isCached(K key) {
        return cacheEntries.containsKey(key);
    }

    /**
     *
     * @return 当前缓存的所有value
     */
    public Iterator<V> iterator() {
        return new LruIter();
    }

    protected class LruIter implements Iterator<V> {
        Node n = head;

        @Override
        public boolean hasNext() {
            return n.next!=null;
        }

        @Override
        public V next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            n=n.next;
            return n.value;
        }
    }
}
