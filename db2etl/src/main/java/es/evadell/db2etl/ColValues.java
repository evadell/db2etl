package es.evadell.db2etl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class ColValues {

	private List<ColValue> list = new ArrayList<ColValue>();

	public List<ColValue> getList() {
		return list;
	}

	public void setList(List<ColValue> list) {
		this.list = list;
	}

	public void add(ColValue cv) {
		list.add(cv);
	}
	
	public ColValue get(String name) {
		Optional<ColValue> o = this.list.stream().filter(s->s.getCol().getName().equals(name)).findFirst();
		if (o.isPresent()) return o.get();
		return null;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ColValues) {
			ColValues other = (ColValues)obj;
			if (this.list.size()!=other.list.size()) return false;
			for(int i=0;i<this.list.size();i++) {
				if (!this.list.get(i).equals(other.getList().get(i))) return false;
			}
			return true;
		}
		return super.equals(obj);
	}

	
}
