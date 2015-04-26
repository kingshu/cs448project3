package relop;

import global.AttrType;
import global.SearchKey;

import java.util.LinkedList;

/**
 * Implements the hash-based join algorithm described in section 14.4.3 of the
 * textbook (3rd edition; see pages 463 to 464). HashIndex is used to partition
 * the tuples into buckets, and HashTableDup is used to store a partition in
 * memory during the matching phase.
 */
public class HashJoin extends Iterator {

	/**
	 * Constructs a hash join, given the left and right iterators and which
	 * columns to match (relative to their individual schemas).
	 */
	private Iterator left = null;
	private Iterator right = null;
	private HashTableDup hashtab = null;
	private Integer lcol;
	private Integer rcol;
	private LinkedList<Tuple> leftQueue = new LinkedList<Tuple>(); // The queue
																	// of Tuples
																	// with the
																	// matching
																	// index of
																	// currentRight
	private Tuple currentRight = null; // The current thing we're doing
										// cross-product with on leftQueue
	private Tuple nextTuple = null; // The next tuple to be returned by getNext
									// as determined by hasNext
	private final int LEFT_FIELD_TYPE;

	public HashJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
		this.left = left;
		this.right = right;
		this.lcol = lcol;
		this.rcol = rcol;
		restart();
		this.schema = Schema.join(left.schema, right.schema);
		if (false && left instanceof IndexScan) {
			// uh
		} else if (false && right instanceof IndexScan) {

		} else {
			hashtab = new HashTableDup();
			LEFT_FIELD_TYPE = left.schema.types[lcol];
			while (left.hasNext()) {
				Tuple t = left.getNext();
				SearchKey key = null;
				assert (t != null);
				switch (LEFT_FIELD_TYPE) {
				case AttrType.INTEGER:
					key = new SearchKey(t.getIntFld(lcol));
					break;
				case AttrType.FLOAT:
					key = new SearchKey(t.getFloatFld(lcol));
					break;
				case AttrType.STRING:
					key = new SearchKey(t.getStringFld(lcol));
					break;
				default:
					assert (false);
				}
				hashtab.add(key, t);
			}
		}
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);
		System.out.println("HashJoin");
		left.explain(depth+1);
		right.explain(depth+1);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		nextTuple = null;
		left.restart();
		right.restart();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return left.isOpen() && right.isOpen();
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		left.close();
		right.close();
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		// TODO what if we haven't consumed yet, don't do anything, just return
		// current state
		if (nextTuple != null) {
			return true;
		}
		if (leftQueue.isEmpty()) {
			if (right.hasNext()) {
				currentRight = right.getNext();
				SearchKey rightIndex = null;
				final int RIGHT_FIELD_TYPE = right.schema.types[rcol];
				if (LEFT_FIELD_TYPE != RIGHT_FIELD_TYPE) {
					// some sort of error
				}
				switch (RIGHT_FIELD_TYPE) {
				case AttrType.INTEGER:
					rightIndex = new SearchKey(currentRight.getIntFld(rcol));
					break;
				case AttrType.FLOAT:
					rightIndex = new SearchKey(currentRight.getFloatFld(rcol));
					break;
				case AttrType.STRING:
					rightIndex = new SearchKey(currentRight.getStringFld(rcol));
					break;
				default:
					assert (false);
				}
				Tuple[] tuples = hashtab.getAll(rightIndex);
				if (tuples == null || tuples.length == 0) {
					return hasNext();
				}
				for (Tuple t : tuples) {
					leftQueue.add(t);
				}
			} else {
				return false;
			}
		}
		nextTuple = Tuple.join(leftQueue.pop(), currentRight, this.schema);
		return true;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (nextTuple == null)
			hasNext();
		if (nextTuple == null)
			throw new IllegalStateException();
		Tuple t = nextTuple;
		nextTuple = null;
		return t;
	}

} // public class HashJoin extends Iterator
