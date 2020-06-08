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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.io.utils.BinaryDataUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
    return Arrays.asList(new Object[][] { { BinaryDataUtils.createFixedBytes(true),
        BinaryDataUtils.createFixedBytes(true), 0, 0, getSchema(Schema.Type.FIXED), 0 } });
  }

  public TestBinaryDataCompare(byte[] b1, byte[] b2, int s1, int s2, Schema schema, Object result) {

    this.b1 = b1;
    this.b2 = b2;
    this.schema = schema;
    this.s1 = s1;
    this.s2 = s2;
    this.result = result;
  }

  @Rule
  public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testCompare() throws IOException {

    Schema i = new Schema.Parser().parse("[\"null\", \"string\"]");

    if (!(result instanceof Integer)) {
      exceptions.expect(Exception.class);
    }

    if ((result instanceof Integer)) {
      Assert.assertEquals(result, BinaryData.compare(b1, s1, b2, s2, schema));

    }

  }

  public static Schema getSchema(Schema.Type type) {

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
      schema = schema.create(Schema.Type.NULL);

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
