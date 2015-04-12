package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

import global.RID;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

  private HeapFile myHeapFile = null;
  private HashScan myHashScan = null;
  private HashIndex myHashIndex = null;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
    
    this.schema = schema;
    this.myHashIndex = index;
    this.myHeapFile = file;
    this.myHashScan = myHashIndex.openScan(key);

  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    for(int i=0;i<depth;i++) {
      System.out.print("\t");
    }
    System.out.println("Does a HashScan");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    this.myHashScan.close();
    this.myHashScan = myHashIndex.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return this.myHashScan != null;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    this.myHashScan.close();
    this.myHashScan = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    
    return (isOpen()) ?
        scan.hasNext()
      : 
        false
    ;
    
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    
    byte[] data = this.myHeapFile.selectRecord(this.myHashScan.getNext());
    return new Tuple(schema, data);

  }

} // public class KeyScan extends Iterator
