import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by tl on 2/9/15.
 */
public abstract class log_parser {

    private static final boolean SEND_TO_KAFKA = true;
    private static final boolean OUTPUT_TO_FILE = true;


    StringBuilder log = new StringBuilder();

    abstract int parse_log(String path, List<String> config, int starting_line) throws Exception;
    abstract void update_conf_file(File file, int new_idx);
    abstract void generate_config_file(File file) throws Exception;


    void output_data() {
        if (OUTPUT_TO_FILE)
            output_to_file();
        if (SEND_TO_KAFKA)
            send_to_kafka();
    }

    void output_to_file() {
        PrintWriter out = null;
        try {
            out = new PrintWriter("./out/outlog.txt");
            out.println(log.toString());
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        finally {
            if (out!=null)
                out.close();
        }
    }

    void send_to_kafka() {
        Properties props = new Properties();
        Producer<String, String> producer = new KafkaProducer<String, String>(props, new StringSerializer(), new StringSerializer());

        String[] lines = log.toString().split("\\n");

        for(String msg: lines) {
            String key = "";
            producer.send(new ProducerRecord<String, String>("the-topic", key, msg));
        }
        producer.close();
    }

    public static void main(String[] args) {

        log_parser parser = new brolog_parser();

        String filename = args[0];
        String dir = args[1];

        String conf_name = dir+"/conf/"+filename.substring(0, filename.indexOf(".log"))+".conf";

        long start = System.currentTimeMillis();
        File config_file = new File(conf_name);
        if (!config_file.exists())
            try {
                parser.generate_config_file(config_file);
            }
            catch (Exception e) {
                System.out.println(e);
                return;
            }


        List<String> lines = new ArrayList<String>();
        try {
            lines = Files.readAllLines(config_file.toPath(), Charset.defaultCharset());
        } catch(Exception e) {
            System.out.println("Invalid file read"+e);
        }
        String path = dir+"/"+filename;
        List<String> config = Arrays.asList(lines.get(0).split("\t"));
        int starting_line = Integer.parseInt(lines.get(1));
        int new_line = 0;
        try {
            new_line = parser.parse_log(path, config, starting_line);
        }
        catch (Exception e) {
            System.out.println("unable to parse log "+e);
            System.out.println(e.getStackTrace());
        }
        parser.update_conf_file(config_file, new_line);
        parser.output_data();
        System.out.println("Took "+(System.currentTimeMillis() - start));
    }
}
