package es.bancamarch.db2etl.model;

public class Column {
	private String name;
	private int colno;
	private String coltype;
	private int length;
	private int scale;
	private boolean nullable;
	private String remarks;
	private char def;
	private int keySeq;
	private String label;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getColno() {
		return colno;
	}

	public void setColno(int colno) {
		this.colno = colno;
	}

	public String getColtype() {
		return coltype;
	}

	public void setColtype(String coltype) {
		this.coltype = coltype;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

	public boolean isNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public char getDef() {
		return def;
	}

	public void setDef(char def) {
		this.def = def;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public void setKeySeq(int keySeq) {
		this.keySeq = keySeq;
	}

	public int getKeySeq() {
		return this.keySeq;
	}
	
}
