/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.utils.BinaryDataUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestBinaryDataCompare {

	private byte[] b1;
	private byte[] b2;
	private int s1;
	private int s2;
	private Schema schema;
	private Object result;

	@Parameterized.Parameters
	public static Collection BufferedChannelParameters() throws Exception {
		return Arrays.asList(new Object[][] { 
			{ null, null, -1, -2, getS(Schema.Type.RECORD), NullPointerException.class},
			{ getBS(Schema.Type.FIXED, true), getBS(Schema.Type.FIXED, true)
				, 0, 0, getS(Schema.Type.FIXED), 0 },
			{getBS(Schema.Type.ARRAY, true), getBS(Schema.Type.ARRAY, false),
					1, 2, getS(Schema.Type.INT), -1},
			{getBS(Schema.Type.INT, true), getBS(Schema.Type.DOUBLE, false)
						, 0, 0, getS(Schema.Type.ARRAY), AvroRuntimeException.class},
			{getBS(Schema.Type.MAP, true), getBS(Schema.Type.DOUBLE, false)
							, 0, 0, getS(Schema.Type.BOOLEAN), 0},
			{getBS(Schema.Type.NULL, true), getBS(Schema.Type.DOUBLE, false)
								, 0, 0, getS(Schema.Type.BYTES), NullPointerException.class},
			{getBS(Schema.Type.NULL, true), getBS(Schema.Type.DOUBLE, false)
									, 0, 0, getS(Schema.Type.FLOAT), NullPointerException.class},
			{getBS(Schema.Type.INT, true), getBS(Schema.Type.INT, false)
									, 0, 0, getS(Schema.Type.INT), 1},
			{getBS(Schema.Type.ENUM, false), getBS(Schema.Type.ENUM, false)
										, 0, 0, getS(Schema.Type.ENUM), 0},
			{getBS(Schema.Type.ENUM, true), getBS(Schema.Type.ENUM, false)
										, 0, 0, getS(Schema.Type.FLOAT), -1},
			{getBS(Schema.Type.FLOAT, true), getBS(Schema.Type.DOUBLE, false)
										, 0, 0, getS(Schema.Type.FLOAT), 1},
			{getBS(Schema.Type.INT, true), getBS(Schema.Type.DOUBLE, false)
										, 0, 0, getS(Schema.Type.MAP), AvroRuntimeException.class},
			{getBS(Schema.Type.INT, true), getBS(Schema.Type.DOUBLE, false)
										, 0, 0, getS(Schema.Type.NULL), AvroRuntimeException.class},
			{getBS(Schema.Type.STRING, true), getBS(Schema.Type.STRING, false)
										, 0, 0, getS(Schema.Type.STRING), -1},
			{getBS(Schema.Type.INT, true), getBS(Schema.Type.DOUBLE, false)
										, 0, 0, getS(Schema.Type.UNION), AvroRuntimeException.class}
		});
	}

	public TestBinaryDataCompare(byte[] b1, byte[] b2, int s1, int s2, Schema schema, Object result) {

		this.b1 = b1;
		this.b2 = b2;
		this.schema = schema;
		this.s1 = s1;
		this.s2 = s2;
		this.result = result;
	}

	@Test
	public void testCompare() {

		try {
			Assert.assertEquals(result, BinaryData.compare(b1, s1, b2, s2, schema));
		} catch (Exception e) {
			Assert.assertEquals(result, e.getClass());
		}

	}

	public static byte[] getBS(Schema.Type type, boolean createB1) throws IOException {

		ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
		byteArrayOutputStream1.reset();
		BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
	   
		  
		  byte[] byteForSchema = null;
  
		  switch (type) {
		  case ARRAY:
			  byteForSchema = BinaryDataUtils.createArrayBytes(createB1);
			  break;
  
		  case BOOLEAN:
				  binaryEncoder1.writeBoolean(createB1);
					binaryEncoder1.flush();
				byteForSchema = byteArrayOutputStream1.toByteArray();	
			  break;
  
		  case BYTES:
			  if (createB1) {
				  binaryEncoder1.writeBytes("TestString".getBytes());
  
			  } else {
				  binaryEncoder1.writeBytes("TestStrinh".getBytes());
			  }
				binaryEncoder1.flush();
			byteForSchema = byteArrayOutputStream1.toByteArray();	
			  break;
  
		  case DOUBLE:
			  if (createB1) {
				  binaryEncoder1.writeDouble(1.0);
  
			  } else {
				  binaryEncoder1.writeDouble(1.1);
			  }
				binaryEncoder1.flush();
			byteForSchema = byteArrayOutputStream1.toByteArray();	
			  break;
  
		  case ENUM:
			  byteForSchema = BinaryDataUtils.createEnumBytes(createB1);
  
		  case FIXED:
			  byteForSchema = BinaryDataUtils.createFixedBytes(createB1);
			  break;
  
		  case FLOAT:
			  if (createB1) {
				  binaryEncoder1.writeFloat(2);
  
			  } else {
				  binaryEncoder1.writeFloat(3);
			  }
				binaryEncoder1.flush();
			byteForSchema = byteArrayOutputStream1.toByteArray();	
			  break;
  
		  case INT:
			  if (createB1) {
				  binaryEncoder1.writeInt(10);
  
			  } else {
				  binaryEncoder1.writeInt(5);
			  }
				binaryEncoder1.flush();
			byteForSchema = byteArrayOutputStream1.toByteArray();	
			  break;
  
		  case LONG:
			  if (createB1) {
				  binaryEncoder1.writeLong(1L);
			  } else {
				  binaryEncoder1.writeLong(2L);
			  }
				binaryEncoder1.flush();
			byteForSchema = byteArrayOutputStream1.toByteArray();	
			  break;
  
		  case MAP:
			  byteForSchema = BinaryDataUtils.createMapBytes(createB1);
			  break;
  
		  case NULL:
			  break;
  
		  case RECORD:
			  byteForSchema =  BinaryDataUtils.createRecordBytes(createB1);
			  break;
  
		  case STRING:
			  if (createB1) {
				  binaryEncoder1.writeString("TestString");
  
			  } else {
				  binaryEncoder1.writeString("TestString1");
			  }
				binaryEncoder1.flush();
			byteForSchema = byteArrayOutputStream1.toByteArray();	
			  break;
  
		  case UNION:
			  byteForSchema =  BinaryDataUtils.createUnionBytes(createB1);
			  break;
  
		  default:
			  Assert.assertEquals(0, 0);
		  }
  
		  return byteForSchema;
  
	  }
  
	  public static Schema getS(Schema.Type type) {
  
		  Schema schema = null;
		  String schemaString = null;
  
		  switch (type) {
		  case ARRAY:
			  schemaString = "{\"type\": \"array\", \"items\": \"int\"}";
			  schema = new Schema.Parser().parse(schemaString);
			  break;
  
		  case BOOLEAN:
			  schema = schema.create(Schema.Type.BOOLEAN);
			  break;
  
		  case BYTES:
			  schema = schema.create(Schema.Type.BYTES);
			  break;
  
		  case DOUBLE:
			  schema = schema.create(Schema.Type.DOUBLE);
			  break;
  
		  case ENUM:
			  schemaString = "{\"type\": \"enum\",\n" + "  \"name\": \"Suit\",\n"
					  + "  \"symbols\" : [\"ARRAY\", \"INT\", \"DIAMONDS\", \"CLUBS\"]\n" + "}";
			  schema = new Schema.Parser().parse(schemaString);
			  break;
  
		  case FIXED:
			  schemaString = "{\"type\" : \"fixed\" , \"name\" : \"bdata\", \"size\" : 1024}";
			  schema = new Schema.Parser().parse(schemaString);
			  break;
  
		  case FLOAT:
			  schema = schema.create(Schema.Type.FLOAT);
			  break;
  
		  case INT:
			  schema = schema.create(Schema.Type.INT);
			  break;
  
		  case LONG:
			  schema = schema.create(Schema.Type.LONG);
			  break;
  
		  case MAP:
			  schemaString = "{\"type\" : \"map\", \"values\" : \"int\"}";
			  schema = new Schema.Parser().parse(schemaString);
			  break;
  
		  case NULL:
			  schema = Schema.create(Schema.Type.NULL);
  
		  case RECORD:
			  schemaString = "{\"namespace\": \"example.avro\",\n" + " \"type\": \"record\",\n" + " \"name\": \"User\",\n"
					  + " \"fields\": [\n" + "     {\"name\": \"name\", \"type\": \"string\"},\n"
					  + "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n"
					  + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" + " ]\n" + "}";
			  schema = new Schema.Parser().parse(schemaString);
			  break;
  
		  case STRING:
			  schema = schema.create(Schema.Type.STRING);
			  break;
  
		  case UNION:
			  schemaString = "{ \n" + "   \"type\" : \"record\", \n" + "   \"namespace\" : \"tutorialspoint\", \n"
					  + "   \"name\" : \"empdetails\", \n" + "   \"fields\" : \n" + "   [ \n"
					  + "      {\"name\" : \"experience\", \"type\": [\"int\", \"null\"]}, {\"name\" : \"age\", \"type\": \"int\"} \n"
					  + "   ] \n" + "}";
			  schema = new Schema.Parser().parse(schemaString);
			  break;
  
		  default:
			  Assert.assertEquals(0, 0);
		  }
  
		  return schema;
  
	  }
  
  }
  