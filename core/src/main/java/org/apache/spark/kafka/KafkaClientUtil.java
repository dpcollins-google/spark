package org.apache.spark.kafka;

import java.util.Base64;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.spark.api.java.function.Function0;

public class KafkaClientUtil {
  public static final String PRODUCER_SUPPLIER_PARAM = "kafka.producer.supplier.override";
  public static final String CONSUMER_SUPPLIER_PARAM = "kafka.consumer.supplier.override";

  private static String serialize(Object obj) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectOutputStream os = new ObjectOutputStream(out);
    os.writeObject(obj);
    return Base64.getEncoder().encodeToString(out.toByteArray());
  }

  private static <T> T deserialize(String source) throws IOException, ClassNotFoundException {
    ByteArrayInputStream in = new ByteArrayInputStream(Base64.getDecoder().decode(source));
    ObjectInputStream is = new ObjectInputStream(in);
    return (T) is.readObject();
  }

  public static String toConsumerSupplierParam(Function0<Consumer<byte[], byte[]>> supplier) throws IOException {
    return serialize(supplier);
  }

  public static Function0<Consumer<byte[], byte[]>> fromConsumerSupplierParam(String supplier) throws IOException, ClassNotFoundException {
    return deserialize(supplier);
  }

  public static String toProducerSupplierParam(Function0<Producer<byte[], byte[]>> supplier) throws IOException {
    return serialize(supplier);
  }

  public static Function0<Producer<byte[], byte[]>> fromProducerSupplierParam(String supplier) throws IOException, ClassNotFoundException {
    return deserialize(supplier);
  }
}
