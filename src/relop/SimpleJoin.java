package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {

	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	private Iterator left;
	private Iterator right;
	private Predicate[] preds;
	private Tuple nextTuple = null;
	private Tuple lTuple = null;
	private Tuple rTuple = null;

	public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
		this.left = left;
		this.right = right;
		this.preds = preds;
		this.schema = Schema.join(left.schema, right.schema);
		this.restart();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);
		System.out.println("Simple Join");
		left.explain(depth + 1);
		right.explain(depth + 1);

	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
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
		if (nextTuple != null)
			return true;
		if (lTuple == null) {
			if (left.hasNext()) {
				lTuple = left.getNext();
			} else {
				return false;
			}
		}
		if (right.hasNext()) {
			rTuple = right.getNext();
		} else {
			right.restart();
			if (right.hasNext()) {
				rTuple = right.getNext();
			} else {
				return false;
			}
			if (left.hasNext()) {
				lTuple = left.getNext();
			} else {
				lTuple = null;
				return false;
			}
		}
		Tuple t = Tuple.join(lTuple, rTuple, schema);
		for (Predicate p : preds) {
			if (p.evaluate(t)) {
				nextTuple = t;
				return true;
			}
		}
		return hasNext();
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if(nextTuple == null)
			hasNext();
		Tuple t = nextTuple;
		nextTuple = null;
		return t;
	}

} // public class SimpleJoin extends Iterator
