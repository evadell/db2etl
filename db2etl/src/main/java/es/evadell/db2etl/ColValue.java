package es.evadell.db2etl;

import es.evadell.db2etl.model.Column;

public class ColValue {
	private Column col;
	private Object value;

	public ColValue(Column col, Object val) {
		this.col = col;
		this.value = val;
	}

	public Column getCol() {
		return col;
	}

	public void setCol(Column col) {
		this.col = col;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ColValue) {
			ColValue other = (ColValue) obj;
			return this.getCol().getName().equals(other.getCol().getName()) && this.value.equals(other.value);
		}
		return super.equals(obj);
	}

}
