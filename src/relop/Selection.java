package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

	private Iterator myIterator;
	private Predicate[] myPredicates;
	private Tuple nextTuple = null;

	/**
	 * Constructs a selection, given the underlying iterator and predicates.
	 */
	public Selection(Iterator iter, Predicate... preds) {
		this.myIterator = iter;
		this.schema = iter.schema;
		this.myPredicates = preds;
		iter.restart();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);
		System.out.println("Does a Selection");
		myIterator.explain(depth + 1);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		myIterator.restart();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return myIterator.isOpen();
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		myIterator.close();
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if(nextTuple != null)
			return true;
		while (myIterator.hasNext()) {
			Tuple t = myIterator.getNext();
			boolean fits = false;
			for (Predicate p : myPredicates) {
				if (p.evaluate(t)) {
					fits = true;
					break;
				}
			}
			if (fits) {
				nextTuple = t;
				break;
			}
		}
		return nextTuple != null;
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

} // public class Selection extends Iterator
