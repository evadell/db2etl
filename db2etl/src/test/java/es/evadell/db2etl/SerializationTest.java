package es.evadell.db2etl;

import java.math.BigDecimal;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import es.evadell.db2etl.model.Column;

public class SerializationTest {

	public static void main(String[] args) {
		ColValues cvs = new ColValues();
		
		Column c1 = new Column();
		c1.setName("colname1");
		ColValue cv1 = new ColValue(c1, "asdf");
		cvs.add(cv1);

		Column c2 = new Column();
		c2.setName("colname2");
		ColValue cv2 = new ColValue(c2, new BigDecimal("1234.567"));
		cvs.add(cv2);
		
		Gson gson = new Gson();
		//Gson gson =new GsonBuilder().setPrettyPrinting().create();
		String json = gson.toJson(cvs);
		System.out.println(json);
		
		//json = "{\"list\":[{\"col\":{\"name\":\"colname1\",\"colno\":0,\"length\":0,\"scale\":0,\"nullable\":false,\"def\":\"\u0000\",\"keySeq\":0},\"value\":\"asdf\"},{\"col\":{\"name\":\"colname2\",\"colno\":0,\"length\":0,\"scale\":0,\"nullable\":false,\"def\":\"\u0000\",\"keySeq\":0},\"value\":1234.567}]}";
		json = "{\"list\":[{\"col\":{\"name\":\"colname1\"},\"value\":\"asdf\"},{\"col\":{\"name\":\"colname2\"},\"value\":1234.567}]}";
		
		ColValues cvs1 = gson.fromJson(json, ColValues.class);
		System.out.println(cvs1.getList().get(1).getValue());
		
		

	}

}
