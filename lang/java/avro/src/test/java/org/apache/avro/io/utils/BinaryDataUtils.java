package org.apache.avro.io.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

public class BinaryDataUtils {

  public static byte[] createFixedBytes(boolean createB1) throws IOException {
    byte[] resultArray = null;
    String schemaString = "{\"type\" : \"fixed\" , \"name\" : \"bdata\", \"size\" : 1024}";
    Schema schema = new Schema.Parser().parse(schemaString);

    if (createB1) {
    	byte[] b1 = new byte[1024];
    	b1[0] = 1;

      GenericFixed a = new GenericData.Fixed(schema, b1);

      SpecificDatumWriter<GenericFixed> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(a, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();
    } else {
    	
    	byte[] b1 = new byte[1024];
    	b1[0] = 2;

      GenericFixed b = new GenericData.Fixed(schema, b1);

      SpecificDatumWriter<GenericFixed> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(b, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();

    }

    return resultArray;
  }

  public static byte[] createMapBytes(boolean createB1) throws IOException {
    byte[] resultArray = null;
    String schemaString = "{\"type\" : \"map\", \"values\" : \"int\"}";
    Schema schema = new Schema.Parser().parse(schemaString);
    HashMap<String, Integer> asd = null;

    if (createB1) {

      asd = new HashMap<>();
      asd.put("Test", 1234);

      SpecificDatumWriter<Map> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(asd, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();

    } else {

      asd = new HashMap<>();
      asd.put("Test", 123456);

      SpecificDatumWriter<Map> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(asd, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();

    }

    return resultArray;
  }

  public static byte[] createUnionBytes(boolean createB1) throws IOException {
    byte[] resultArray = null;
    String schemaString = "{\"type\" : \"record\", \n" + "   \"namespace\" : \"tutorialspoint\", \n"
        + "   \"name\" : \"empdetails\", \n" + "   \"fields\" : \n" + "   [ \n"
        + "      {\"name\" : \"experience\", \"type\": [\"int\", \"null\"]}, {\"name\" : \"age\", \"type\": \"int\"} \n"
        + "   ] \n" + "}";
    Schema schema = new Schema.Parser().parse(schemaString);

    if (createB1) {

      GenericRecord a = new GenericData.Record(schema);
      a.put("experience", 18);
      a.put("age", 0);

      SpecificDatumWriter<GenericRecord> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(a, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();

    } else {
    	
    	schemaString = "{\"type\" : \"record\", \n" + "   \"namespace\" : \"tutorialspoint111\", \n"
                + "   \"name\" : \"empdetails\", \n" + "   \"fields\" : \n" + "   [ \n"
                + "      {\"name\" : \"experiences\", \"type\": [\"int\", \"null\"]}, {\"name\" : \"age\", \"type\": \"int\"} \n"
                + "   ] \n" + "}";
    	schema = new Schema.Parser().parse(schemaString);

      GenericRecord b = new GenericData.Record(schema);
      b.put("experiences", 14);
      b.put("age", 1234);
     

      SpecificDatumWriter<GenericRecord> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(b, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();

    }

    return resultArray;
  }

  public static byte[] createEnumBytes(boolean createB1) throws IOException {
    byte[] resultArray = null;
    String schemaString = "{\"type\": \"enum\",\n" + "  \"name\": \"Suit\",\n"
        + "  \"symbols\" : [\"ARRAY\", \"INT\", \"DIAMONDS\", \"CLUBS\"]\n" + "}";
    Schema schema = new Schema.Parser().parse(schemaString);

    if (createB1) {

      SpecificDatumWriter<Enum> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(Schema.Type.ARRAY, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();

    } else {

      SpecificDatumWriter<Enum> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(Schema.Type.INT, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();

    }

    return resultArray;
  }

  public static byte[] createArrayBytes(boolean createB1) throws IOException {

    byte[] resultArray = null;
    String schemaString = "{\"type\": \"array\", \"items\": \"int\"}";
    Schema schema = new Schema.Parser().parse(schemaString);

    if (createB1) {
      Array<Integer> user1 = new GenericData.Array<Integer>(2, schema);
      user1.add(-2);
      user1.add(-1);

      SpecificDatumWriter<Array> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(user1, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();

    } else {

      Array<Integer> user2 = new GenericData.Array<Integer>(5, schema);
      user2.add(-3);
      user2.add(2);
      user2.add(2);
      user2.add(3);
      user2.add(4);

      SpecificDatumWriter<Array> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(user2, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();

    }

    return resultArray;
  }

  public static byte[] createRecordBytes(boolean createB1) throws IOException {

    byte[] resultArray = null;

    String schemaString = "{\"namespace\": \"example.avro\",\n" + " \"type\": \"record\",\n" + " \"name\": \"User\",\n"
        + " \"fields\": [\n" + "     {\"name\": \"name\", \"type\": \"string\"},\n"
        + "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n"
        + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" + " ]\n" + "}";
    Schema schema = new Schema.Parser().parse(schemaString);

    if (createB1) {

      GenericRecord user1 = new GenericData.Record(schema);
      user1.put("name", "Ben");
      user1.put("favorite_number", 7);

      SpecificDatumWriter<GenericRecord> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(user1, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();

    } else {
      GenericRecord user2 = new GenericData.Record(schema);
      user2.put("name", "Ben");
      user2.put("favorite_number", 7);
      user2.put("favorite_color", "red");

      SpecificDatumWriter<GenericRecord> datumWriter1 = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
      byteArrayOutputStream1.reset();
      BinaryEncoder binaryEncoder1 = new EncoderFactory().binaryEncoder(byteArrayOutputStream1, null);
      datumWriter1.write(user2, binaryEncoder1);
      binaryEncoder1.flush();
      resultArray = byteArrayOutputStream1.toByteArray();

    }

    return resultArray;

  }

}
