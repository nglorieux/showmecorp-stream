import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

public class infme {
    private static DatumReader<GenericRecord> datumReader;

    public static String ByteArrayToString(byte[] ba) {
        return DatatypeConverter.printHexBinary(ba);
    }

    public static void main(String[] args) throws Exception {
        String truststorepassword = null;
        String truststorelocation = null;
        SparkConf conf = (new SparkConf()).setAppName("AMR-kafka-sandbox");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(10000L));
        if (args.length > 0) {
            System.out.println("argument=" + args[0]);

            for (int i = 0; i < args.length; ++i) {
                if (args[i].startsWith("truststorepwd")) {
                    String[] parts = args[i].split("=", 2);
                    if (parts.length == 2) {
                        truststorepassword = parts[1];
                    }
                }
                else if (args[i].startsWith("truststorelocation")) {
                    String[] parts = args[i].split("=", 2);
                    if (parts.length == 2) {
                        truststorelocation = parts[1];
                    }
                }
            }
        }

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers",
                "zkwssbbdpdk01.kw.zain.com:9093,zkwssbbdpdk02.kw.zain.com:9093,zkwssbbdpdk03.kw.zain.com:9093,zkwssbbdpdk04.kw.zain.com:9093,zkwssbbdpdk05.kw.zain.com:9093,zkwssbbdpdk06.kw.zain.com:9093,zkwssbbdpdk07.kw.zain.com:9093,zkwssbbdpdk08.kw.zain.com:9093,zkwssbbdpdk09.kw.zain.com:9093 ");
        kafkaParams.put("sasl.kerberos.service.name", "kafka");
        kafkaParams.put("security.protocol", "SASL_SSL");
        kafkaParams.put("sasl.mechanism", "GSSAPI");
        if (truststorelocation != null) {
            kafkaParams.put("ssl.truststore.location", truststorelocation);
        }
        if (truststorepassword != null) {
            kafkaParams.put("ssl.truststore.password", truststorepassword);
        }
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("group.id", "up_tst_ast_xdr_shm");
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("session.timeout.ms", "30000");
        kafkaParams.put("debug", "true");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaParams.put("schema.registry.url", "");
        kafkaParams.put("specific.avro.reader", "true");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("client.id", "showme_usr@corp.mtc.com.kw");
        Collection<String> topics = Arrays.asList("up");
        JavaInputDStream<ConsumerRecord<String, byte[]>> stream = KafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

        stream.foreachRDD((rdd, time) -> {
            try {
                if (!rdd.isEmpty()) {
                    // ✅ Step 1: Capture offset ranges BEFORE any transformation
                    OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

                    // ✅ Step 2: Do your transformation on the RDD
                    JavaRDD<String> processedRDD = rdd.map(record -> {
                        return convertAvroValue(record.value()) + "," + record.offset() + "," + record.partition();
                    }).map(x -> {
                        String[] parts = x.split(",");

                        return parts[0] + "," + parts[1] + "," + parts[2] + "," + parts[3] + ","
                                + parts[4] + "," + parts[5] + "," + parts[6] + "," + parts[7] + ","
                                + parts[8] + "," + parts[9] + "," + parts[10] + "," + parts[11] + ","
                                + parts[12] + "," + parts[13] + "," + parts[14] + "," + parts[15] + ","
                                + parts[30] + "," + parts[31] + "," + parts[18] + "," + parts[19] + ","
                                + parts[20] + "," + parts[21] + "," + parts[22] + "," + parts[16] + ","
                                + parts[17] + "," + parts[23] + "," + parts[24] + "," + parts[25] + ","
                                + parts[26] + "," + parts[27] + "," + parts[28] + "," + parts[29];
                    });

                    // ✅ Step 3: Save result
                    String location = buildOutputPath(); // your timestamp-based logic
                    processedRDD.saveAsTextFile(location);

                    // ✅ Step 4: Commit offsets after successful processing
                    ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        });



        ssc.start();
        ssc.awaitTermination();
    }

    private static String buildOutputPath()
    {
        LocalDateTime now = LocalDateTime.now();
        String year = Integer.toString(now.getYear());
        String month = (now.getMonthValue() < 10 ? "0" : "") + Integer.toString(now.getMonthValue());
        String day = (now.getDayOfMonth() < 10 ? "0" : "") + Integer.toString(now.getDayOfMonth());
        String hour = (now.getHour() < 10 ? "0" : "") + Integer.toString(now.getHour());
        String minute = Integer.toString(now.getMinute());
        String second = Integer.toString(now.getSecond());
        String millis = Integer.toString(now.get(ChronoField.MILLI_OF_SECOND));
        String location = "hdfs:///data/showme/hive/foundation_layer_hive.db/esdr/date_hour=" + year + month + day
                + hour + "/" + minute + "-" + second + "-" + millis;
        return location;
    }
    private static String convertAvroValue(byte[] payload) {
        int headerlength = 27;
        String[] data = new String[30];
        Arrays.fill(data, "");

        try {
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(payload, headerlength,
                    payload.length - headerlength, null);
            GenericRecord record = (GenericRecord) datumReader.read(null, binaryDecoder);

            Long long_last_cell;
            try {
                long_last_cell = Long.parseLong(record.get("session_duration").toString()) * 1000000L;
                data[0] = long_last_cell.toString();
            } catch (NullPointerException var28) {
                data[0] = "0";
            }

            try {
                data[1] = record.get("end_date_utc").toString();
            } catch (NullPointerException var27) {
            }

            data[2] = "";

            try {
                data[3] = record.get("msisdn").toString();
            } catch (NullPointerException var26) {
            }

            try {
                data[4] = record.get("imei").toString();
            } catch (NullPointerException var25) {
            }

            try {
                data[5] = record.get("imei").toString().substring(0,
                        Math.min(8, record.get("imei").toString().length()));
            } catch (NullPointerException var24) {
            }

            try {
                data[6] = record.get("first_src_node_ip").toString();
            } catch (NullPointerException e) {
            }

            try {
                data[7] = record.get("last_location").toString();
            } catch (NullPointerException var23) {
                data[7] = "0";
            }

            try {
                data[8] = record.get("volumel3_ul").toString();
            } catch (NullPointerException var22) {
                data[8] = "0";
            }

            try {
                data[9] = record.get("volumel3_dl").toString();
            } catch (NullPointerException var21) {
                data[9] = "0";
            }

            try {
                data[10] = record.get("application").toString();
            } catch (NullPointerException var20) {
            }

            try {
                data[11] = record.get("category").toString();
            } catch (NullPointerException var19) {
            }

            String last_cell;
            try {
                long_last_cell = Long.parseLong(record.get("first_location").toString());

                for (last_cell = Long.toBinaryString(long_last_cell); last_cell.length() < 60; last_cell = "0"
                        + last_cell) {
                }

                if (last_cell.startsWith("0000")) {
                    data[12] = Long.parseLong(last_cell.substring(last_cell.length() - 28, last_cell.length() - 8), 2)
                            + "";
                    data[13] = Long.parseLong(last_cell.substring(last_cell.length() - 8, last_cell.length()), 2) + "";
                } else if (!last_cell.startsWith("0001") && !last_cell.startsWith("0010")) {
                    data[12] = "0";
                    data[13] = "0";
                } else {
                    data[12] = Long.parseLong(last_cell.substring(last_cell.length() - 32, last_cell.length() - 16), 2)
                            + "";
                    data[13] = Long.parseLong(last_cell.substring(last_cell.length() - 16, last_cell.length()), 2) + "";
                }
            } catch (NullPointerException var30) {
                data[12] = "0";
                data[13] = "0";
            }

            try {
                long_last_cell = Long.parseLong(record.get("last_location").toString());

                for (last_cell = Long.toBinaryString(long_last_cell); last_cell.length() < 60; last_cell = "0"
                        + last_cell) {
                }

                if (last_cell.startsWith("0000")) {
                    data[14] = Long.parseLong(last_cell.substring(last_cell.length() - 28, last_cell.length() - 8), 2)
                            + "";
                    data[15] = Long.parseLong(last_cell.substring(last_cell.length() - 8, last_cell.length()), 2) + "";
                } else if (!last_cell.startsWith("0001") && !last_cell.startsWith("0010")) {
                    data[14] = "0";
                    data[15] = "0";
                } else {
                    data[14] = Long.parseLong(last_cell.substring(last_cell.length() - 32, last_cell.length() - 16), 2)
                            + "";
                    data[15] = Long.parseLong(last_cell.substring(last_cell.length() - 16, last_cell.length()), 2) + "";
                }
            } catch (NullPointerException var29) {
                data[14] = "0";
                data[15] = "0";
            }

            try {
                data[16] = record.get("throughput_transferl4_ul").toString();
            } catch (NullPointerException var18) {
                data[16] = "0";
            }

            try {
                data[17] = record.get("throughput_transferl4_dl").toString();
            } catch (NullPointerException var17) {
                data[17] = "0";
            }

            try {
                data[18] = record.get("hostname").toString();
            } catch (NullPointerException var16) {
                data[18] = "";
            }

            try {
                data[19] = record.get("user_ip").toString();
            } catch (NullPointerException var15) {
                data[19] = "";
            }

            try {
                data[20] = record.get("server_ip").toString();
            } catch (NullPointerException var14) {
                data[20] = "";
            }

            try {
                data[21] = record.get("protocol").toString();
            } catch (NullPointerException var13) {
                data[21] = "";
            }

            try {
                data[22] = record.get("application_attribute").toString();
            } catch (NullPointerException var12) {
                data[22] = "";
            }

            try {
                data[23] = record.get("activitydurationl4_ul").toString();
            } catch (NullPointerException var11) {
                data[23] = "0";
            }

            try {
                data[24] = record.get("activitydurationl4_dl").toString();
            } catch (NullPointerException var10) {
                data[24] = "0";
            }

            try {
                data[25] = record.get("first_mcc").toString();
            } catch (NullPointerException var9) {
                data[25] = "";
            }

            try {
                data[26] = record.get("first_mnc").toString();
            } catch (NullPointerException var8) {
                data[26] = "";
            }
            try {
                data[27] = record.get("first_rat").toString();
            } catch (NullPointerException var8) {
                data[27] = "";
            }
            try {
                data[28] = record.get("last_rat").toString();
            } catch (NullPointerException var8) {
                data[28] = "";
            }
            try {
                data[29] = record.get("imsi").toString();
            } catch (NullPointerException var8) {
                data[29] = "";
            }
        } catch (EOFException var31) {
            var31.printStackTrace();
        } catch (ArrayIndexOutOfBoundsException var32) {
        } catch (IOException var33) {
            var33.printStackTrace();
        }

        return String.join(",", data);
    }

    static {
        try {
            Schema schema = (new Schema.Parser()).parse(new File("schema.avsc"));

            try {
                byte[] schemaHash = SchemaNormalization.parsingFingerprint("CRC-64-AVRO", schema);
                String schemaHashString = Hex.encodeHexString(schemaHash);
                System.out.println("schemaHashString:" + schemaHashString);
            } catch (NoSuchAlgorithmException var2) {
                var2.printStackTrace();
            }

            datumReader = new GenericDatumReader(schema);
        } catch (IOException var3) {
            System.out.println("datumReader error");
            System.out.println(var3.toString());
        }

    }
}