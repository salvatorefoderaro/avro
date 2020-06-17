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
package org.apache.avro.specific;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSpecificDataGetClass {

  private Schema schema;
  private Object result;
  private static final String ERROR = "Not a Specific class: class java.lang.Exception";
  Collection<String> array = new LinkedList<String>();
  Map<String, String> map = new HashMap<>();
  Map<Integer, Integer> mapInt = new HashMap<>();

  @Parameterized.Parameters
  public static Collection BufferedChannelParameters() throws Exception {
    return Arrays.asList(new Object[][] { 
    	
      {getS(Schema.Type.INT), Integer.TYPE},
      {getS(Schema.Type.UNION), null},
      {getS(Schema.Type.ENUM), null},
      {getS(Schema.Type.ARRAY), List.class},
      {getS(Schema.Type.MAP), Map.class},
      {getS(Schema.Type.STRING), CharSequence.class},
      {getS(Schema.Type.BYTES), ByteBuffer.class},
      {getS(Schema.Type.LONG), Long.TYPE},
      {getS(Schema.Type.FLOAT), Float.TYPE},
      {getS(Schema.Type.DOUBLE), Double.TYPE},
      {getS(Schema.Type.BOOLEAN), Boolean.TYPE},
      {getS(Schema.Type.NULL), Void.TYPE},

    	});
  }

  public TestSpecificDataGetClass(Schema schema, Object result) {
    this.schema = schema;
    this.result = result;
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
		  break;

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


  @Test
  public void testSkipLong() throws IOException, NoSuchFieldException, SecurityException { 
    
    try {
    	Assert.assertEquals(result, SpecificData.get().getClass(schema));
    } catch (Exception e) {
    	Assert.assertEquals(result, e.getClass());
    }

  }

}
