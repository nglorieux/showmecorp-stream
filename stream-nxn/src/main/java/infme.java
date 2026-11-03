import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.*;


public class infme {
    private static ArrayList<Integer> index = new ArrayList<Integer>();
    private static DatumReader<GenericRecord> datumReader;
    static
    {
        try {
            Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
            datumReader = new GenericDatumReader<GenericRecord>(schema);
        }
        catch(IOException e)
        {
            System.out.println("datumReader error");
            System.out.println(e.toString());
        }
    }
    // the index of the columns that we need to consume from each record
    public static void main(String[] args) throws Exception{
        // adding the index of required columns in array index
        String password = null;
        SparkConf conf = new SparkConf().setAppName("AMR-kafka-sandbox");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc,new Duration (10000));

        if (args.length > 0) {
            System.out.println("argument="+args[0]);
            for (int i = 0; i < args.length; i++)
            {
                // "trustorepwd" is the old spelling, kept so existing launch scripts still work
                if (args[i].startsWith("truststorepwd=") || args[i].startsWith("trustorepwd=")) {
                    password = args[i].split("=", 2)[1];
                }
            }
        }
// create spark context and spark streaming context with batch of every 20 secs
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.222.24.246:30200,10.222.24.248:30200,10.222.24.250:30200");
        //kafkaParams.put("bootstrap.servers", "zkwbdprodw11.kw.zain.com:9093,zkwbdprodw12.kw.zain.com:9093,zkwbdprodw13.kw.zain.com:9093,zkwbdprodw14.kw.zain.com:9093,zkwbdprodw15.kw.zain.com:9093");
        kafkaParams.put("security.protocol", "PLAINTEXT");
        kafkaParams.put("sasl.kerberos.service.name", "kafka");
        kafkaParams.put("sasl.mechanism", "GSSAPI");
        kafkaParams.put("ssl.truststore.location", "/opt/cloudera/security/keystore/truststore.jks");
        if (password != null) {
            kafkaParams.put("ssl.truststore.password", password);
        }
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("group.id", "exfo.consumer.esdr");
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

        Collection<String> topics = Arrays.asList("up");
        JavaInputDStream<ConsumerRecord<String, byte[]>> stream = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent(),ConsumerStrategies.<String, byte[]>Subscribe(topics, kafkaParams));
        //stream.print();

        // subscribe to topic and create a direct stream
        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            JavaRDD<String> lines= rdd.map(x-> convertAvroValue(x.value())+","+Long.toString(x.offset())+","+Long.toString(x.partition()));
        //lines.print();
        // extracting value , offset and partition for each consumed message and adding offset and partition to the CSV message in the end of the value
        JavaRDD<String> linesnn=lines.map(x -> x.split(",")[0]+","+x.split(",")[1]+","+x.split(",")[2]+","+x.split(",")[3]+","+x.split(",")[4]+","+x.split(",")[5] +","+x.split(",")[6]+","+x.split(",")[7]+","+x.split(",")[8]+","+x.split(",")[9]+","+x.split(",")[10]+","+x.split(",")[11]+","+x.split(",")[12]+","+x.split(",")[13]+","+x.split(",")[14]+","+x.split(",")[15]+","+x.split(",")[17]+","+x.split(",")[18]+","+x.split(",")[16]);
        //linesnn.print();
        // extracting desired columns only by using previously specified index and concatenating the by comma to be the output
        // as follow value1,value2,.....,offset,partition
            JavaRDD<String> x = linesnn;
            LocalDateTime now = LocalDateTime.now();
            String year = Integer.toString(now.getYear());
            String month = (now.getMonthValue()<10?"0":"")+Integer.toString(now.getMonthValue());
            String day = (now.getDayOfMonth()<10?"0":"")+Integer.toString(now.getDayOfMonth());
            String hour = (now.getHour()<10?"0":"")+Integer.toString(now.getHour());
            String minute = Integer.toString(now.getMinute());
            String second = Integer.toString(now.getSecond());
            String millis = Integer.toString(now.get(ChronoField.MILLI_OF_SECOND));

            String location = "hdfs:///data/showme/hive/foundation_layer_hive.db/esdr/date_hour="+year+month+day+hour+"/"+minute+"-"+second+"-"+millis;
            String y=x.name();
            x.saveAsTextFile(location);
            // Save the Kafka position only after the batch is written
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
            //writing the consumed batch into HDFS in the following directory /data/sandbox/sandbox_nxn.hive.db/cem_table_year_month_day_hour/minute-second-millis.tx according to writing time timestamp

// aggiunto io 22/02/2021			/data/showme/spark_sql/nxn_test/cem_table_"+year+"_"+month+"_"+day+"_"+hour+"/"+minute+"-"+second+"-"+millis+".txt"
        });

        ssc.start();
        ssc.awaitTermination();
    }

    private static String convertAvroValue(byte[] payload) {
        int headerlength = 27;
        String [] data = new String[17];
        Arrays.fill(data, "");
        try {
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(payload, headerlength, payload.length - headerlength, null);
            GenericRecord record = datumReader.read(null, binaryDecoder);
            // #0 - 16 -> session_duration
            try
            {
                Long session_duration = Long.parseLong(record.get("session_duration").toString()) * 1000000000;
                data[0] = session_duration.toString();
            }
            catch (java.lang.NullPointerException e)
            {
                data[0] = "0";
            }

            // #1 - 17 -> last_timestamp_utc
            try
            {
                data[1] = record.get("last_timestamp_utc").toString();
            }
            catch (java.lang.NullPointerException e)
            {
            }
            // #2 - 26 -> roaming
            try
            {
                data[2] = record.get("roaming").toString();
            }
            catch (java.lang.NullPointerException e)
            {
            }
            // #3 - 44 -> msisdn
            try
            {
                data[3] = record.get("msisdn").toString();
            }
            catch (java.lang.NullPointerException e)
            {
            }
            // #4 - 45 -> IMEI
            try
            {
                data[4] = record.get("imei").toString();
            }
            catch (java.lang.NullPointerException e)
            {
            }
            // #5 - 46 -> first 8 digits of IMEI field
            try
            {
                data[5] = record.get("imei").toString().substring(0, Math.min(8, record.get("imei").toString().length()));
            }
            catch (java.lang.NullPointerException e)
            {
            }
            // #6 - 59 -> last_location_type
            try
            {
                data[6] = record.get("last_location_type").toString();
            }
            catch (java.lang.NullPointerException e)
            {
            }
            // #7 - 61 -> last_cell
            try
            {
                data[7] =record.get("last_cell").toString();
            }
            catch (java.lang.NullPointerException e)
            {
                data[7] = "0";
            }
            // #8 - 74 -> ul_volume_all
            try
            {
                Long volumeul = Long.parseLong(record.get("ul_volume_all").toString()) * 1024;
                data[8] =volumeul.toString();
            }
            catch (java.lang.NullPointerException e)
            {
                data[8] = "0";
            }
            // #9 - 75 -> dl_volume_all
            try
            {
                Long volumedl = Long.parseLong(record.get("dl_volume_all").toString()) * 1024;
                data[9] = volumedl.toString();
            }
            catch (java.lang.NullPointerException e)
            {
                data[9] = "0";
            }
            // #10 - 90 -> content_provider
            try
            {
                data[10] = record.get("content_provider").toString();
            }
            catch (java.lang.NullPointerException e)
            {
            }
            // #11 - 91 -> content_category
            try
            {
                data[11] = record.get("content_category").toString();
            }
            catch (java.lang.NullPointerException e)
            {
            }
            // #12 - 53 -> enodebid decoded based on latest implemented logic of first_cell schema field
            // #13 - 55 -> cellid decoded based on latest implemented logic of first_cell schema field
            try
            {
                Long long_first_cell = Long.parseLong(record.get("first_cell").toString());
                String first_cell = Long.toBinaryString(long_first_cell);
                while (first_cell.length() < 60)
                {
                    first_cell = "0" + first_cell;
                }

                if (first_cell.startsWith("0000"))
                {
                    data[12] = Long.parseLong(first_cell.substring(first_cell.length() - 28, first_cell.length() - 8), 2)+"";
                    data[13] = Long.parseLong(first_cell.substring(first_cell.length() - 8, first_cell.length()), 2)+"";
                }
                else if (first_cell.startsWith("0001") || first_cell.startsWith("0010"))
                {
                    data[12] = Long.parseLong(first_cell.substring(first_cell.length() - 32, first_cell.length() - 16), 2)+"";
                    data[13] = Long.parseLong(first_cell.substring(first_cell.length() - 16, first_cell.length()), 2)+"";
                }
                else
                {
                    data[12] = "0";
                    data[13] = "0";
                }
            }
            catch (java.lang.NullPointerException e)
            {
                data[12] = "0";
                data[13] = "0";
            }
            // #14 - 58 -> enodebid decoded based on latest implemented logic of last_cell schema field
            // #15 - 60 -> cellid decoded based on latest implemented logic of last_cell schema field
            try
            {
                Long long_last_cell = Long.parseLong(record.get("last_cell").toString());
                String last_cell = Long.toBinaryString(long_last_cell);
                while (last_cell.length() < 60)
                {
                    last_cell = "0" + last_cell;
                }

                if (last_cell.startsWith("0000"))
                {
                    data[14] = Long.parseLong(last_cell.substring(last_cell.length() - 28, last_cell.length() - 8), 2)+"";
                    data[15] = Long.parseLong(last_cell.substring(last_cell.length() - 8, last_cell.length()), 2)+"";
                }
                else if (last_cell.startsWith("0001") || last_cell.startsWith("0010"))
                {
                    data[14] = Long.parseLong(last_cell.substring(last_cell.length() - 32, last_cell.length() - 16), 2)+"";
                    data[15] = Long.parseLong(last_cell.substring(last_cell.length() - 16, last_cell.length()), 2)+"";
                }
                else
                {
                    data[14] = "0";
                    data[15] = "0";
                }

            }
            catch (java.lang.NullPointerException e)
            {
                data[14] = "0";
                data[15] = "0";
            }
            try {
                data[16] = record.get("host").toString();
            } catch (java.lang.NullPointerException e) {
                data[16] = "";
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        catch (java.lang.ArrayIndexOutOfBoundsException e)
        {
            //System.out.println("bad data");
        }
        return String.join(",", data);
    }
}
