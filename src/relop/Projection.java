package relop;

import global.AttrType;

import java.util.ArrayList;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

	private Iterator iter = null;
	private Integer[] fields;
	private boolean hadNext = false;

	/**
	 * Constructs a projection, given the underlying iterator and field numbers.
	 */
	public Projection(Iterator iter, Integer... fields) {
		this.iter = iter;
		this.fields = fields;
		this.schema = new Schema(this.fields.length);
		int index = 0;
		for(Integer i : this.fields) {
			/*switch(t.schema.fieldType(index)) {
			case AttrType.FLOAT:
				System.out.println("Index("+index+") type(FLOAT)");
				break;
			case AttrType.INTEGER:
				System.out.println("Index("+index+") type(INTEGER)");
				break;
			case AttrType.STRING:
				System.out.println("Index("+index+") type(STRING)");
				break;
			}*/
			this.schema.initField(index, iter.schema, i);
			index++;
		}
		iter.restart();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);
		System.out.println("Projection");
		iter.explain(depth+1);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		iter.restart();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return iter.isOpen();
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		iter.close();
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		this.hadNext = iter.hasNext();
		return hadNext;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if(!hadNext)
			hasNext();
		if(!hadNext)
			throw new IllegalStateException();
		Tuple t = iter.getNext();
		int index = 0;
		Tuple newT = new Tuple(this.schema);
		for(Integer i : this.fields) {
			newT.setField(index++,t.getField(i));
		}
		return newT;
	}

} // public class Projection extends Iterator
