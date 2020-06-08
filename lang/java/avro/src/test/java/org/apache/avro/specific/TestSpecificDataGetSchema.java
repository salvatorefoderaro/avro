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
import java.util.Map;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSpecificDataGetSchema {

  private String b1;
  private String b2;
  private String schema;
  private int s1;
  private int s2;
  private Object result;
  Collection<String> array = new LinkedList<String>();
  Map<String, String> map = new HashMap<>();
  Schema a;

  @Parameterized.Parameters
  public static Collection BufferedChannelParameters() throws Exception {
    return Arrays.asList(new Object[][] { { "correctSchema", "correctSchemaEqual", "int", 0, 0, 0 },
        { "correctSchema", "correctSchema", "float", 0, 0, -1 } });
  }

  public TestSpecificDataGetSchema(String b1, String b2, String schema, int s1, int s2, Object result) {

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
  public void testSkipLong() throws IOException, NoSuchFieldException, SecurityException {

    if (!(result instanceof Integer)) {
      exceptions.expect(Exception.class);
    }

    Field attributeField = null;
    java.lang.reflect.Type classType = String.class;
    Schema.Type schemaType = null;
    switch (this.schema) {
    case "int":
      classType = Integer.class;
      schemaType = Schema.Type.INT;
      break;

    case "null":
      classType = Void.class;
      schemaType = Schema.Type.NULL;
      break;

    case "long":
      classType = Long.class;
      schemaType = Schema.Type.LONG;
      break;

    case "float":
      classType = Float.class;
      schemaType = Schema.Type.FLOAT;
      break;

    case "double":
      classType = Double.class;
      schemaType = Schema.Type.DOUBLE;
      break;

    case "bytes":
      classType = ByteBuffer.class;
      schemaType = Schema.Type.BYTES;
      break;

    case "string":
      classType = String.class;
      schemaType = Schema.Type.STRING;
      break;

    case "map":
      attributeField = TestSpecificDataGetSchema.class.getDeclaredField("map");
      classType = (ParameterizedType) attributeField.getGenericType();
      schemaType = Schema.Type.MAP;
      break;

    case "array":
      attributeField = TestSpecificDataGetSchema.class.getDeclaredField("arr");
      classType = (ParameterizedType) attributeField.getGenericType();
      schemaType = Schema.Type.ARRAY;
      break;

    default:

    }

    Assert.assertEquals(schemaType, SpecificData.get().getSchema(classType).getType());

  }

}
