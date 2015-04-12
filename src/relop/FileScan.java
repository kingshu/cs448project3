package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

	private HeapFile myHeapFile;
	private HeapScan myHeapScan = null;
	private RID lastRID = null;

	/**
	 * Constructs a file scan, given the schema and heap file.
	 */
	public FileScan(Schema schema, HeapFile file) {
		this.schema = schema;
		this.myHeapFile = file;
		myHeapScan = myHeapFile.openScan();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		for(int i=0;i<depth;i++) {
			System.out.print("\t");
		}
		System.out.println("Does a FileScan");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		myHeapScan.close();
		myHeapScan = myHeapFile.openScan();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return myHeapScan != null;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		myHeapScan.close();
		myHeapScan = null;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if(myHeapScan != null)
			return myHeapScan.hasNext();
		return false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		lastRID = new RID();
		byte[] data = myHeapScan.getNext(lastRID);
		return new Tuple(schema,data);
	}

	/**
	 * Gets the RID of the last tuple returned.
	 */
	public RID getLastRID() {
		return this.lastRID;
	}

} // public class FileScan extends Iterator
