import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.*;

import java.io.File;
import java.nio.file.*;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * Created by tl on 2/9/15.
 */
public abstract class log_parser {

    private static final boolean SEND_TO_KAFKA = true;
    private static final boolean OUTPUT_TO_FILE = true;


    protected List<Map<String, String>> logs;

    abstract int parse_log(String path, List<String> config, int starting_line) throws Exception;
    abstract void update_conf_file(File file, int new_idx);
    abstract void generate_config_file(File file) throws Exception;
    abstract List<String> get_config_strings(List<String> config_lines) throws Exception;
    abstract int get_starting_line(List<String> config_lines) throws Exception;



    void output_data() {
        if (OUTPUT_TO_FILE)
            output_to_file();
        if (SEND_TO_KAFKA)
            send_to_kafka();
    }

    private static String get_kafka_IP() throws Exception{
        URL url = new URL("http://vivaldi.crhc.illinois.edu:4001/v2/keys/services/kafka");
        String kafkaIP = "";
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"))) {
            String line = reader.readLine();
            Map<String, Object> values = new Gson().fromJson(line, Map.class);
            kafkaIP = (String )((Map<String, Object>)values.get("node")).get("value");
        }
        return kafkaIP;
    }

    void output_to_file() {
        for (Map<String, String> entry : logs) {
            System.out.println(map_to_string(entry));
        }
    }

    static String map_to_string(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<String, String> entry : map.entrySet())
            sb.append("\"" + entry.getKey() + "\": \"" + entry.getValue() + "\", ");
        sb.deleteCharAt(sb.length()-1); sb.deleteCharAt(sb.length()-1);
        sb.append("}");
        return sb.toString();
    }

    void send_to_kafka() {
        Properties props = new Properties();
        String kafkaIP = null;
        try {
            kafkaIP = get_kafka_IP();
        } catch (Exception e) {
            System.out.println("unable to get kafka IP address "+e);
            return;
        }
        props.put("bootstrap.servers", kafkaIP);
        Producer<String, String> producer = new KafkaProducer(props, new StringSerializer(), new StringSerializer());


        for (Map<String, String> entry : logs) {
            String key = "";
            String msg = map_to_string(entry);
            producer.send(new ProducerRecord<String, String>("the-topic", key, msg));
        }
        producer.close();
    }

    public static void log_event(String filename, String dir) {
        log_parser parser = new brolog_parser();
        parser.logs = new ArrayList<>();

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


        List<String> config_lines = new ArrayList<String>();
        try {
            config_lines = Files.readAllLines(config_file.toPath(), Charset.defaultCharset());
        } catch(Exception e) {
            System.out.println("Invalid file read for config_strings"+e);
            return;
        }

        List<String> config_strings = null;
        try {
            config_strings = parser.get_config_strings(config_lines);
        } catch (Exception e) {
            System.out.println("Unable to parse config_strings file for strings" + e);
            return;
        }

        int starting_line = 0;
        try {
            starting_line = parser.get_starting_line(config_lines);
        } catch (Exception e) {
            System.out.println("Unable to parse config_strings file for starting line" + e);
            return;
        }

        String path = dir+"/"+filename;
        int lines_parsed = 0;
        try {
            lines_parsed = parser.parse_log(path, config_strings, starting_line);
        }
        catch (Exception e) {
            System.out.println("unable to parse log "+e);
            System.out.println(e.getStackTrace().toString());
        }
        parser.update_conf_file(config_file, lines_parsed);
        parser.output_data();
        System.out.println("Took "+(System.currentTimeMillis() - start));
    }

    public static void watch(String dir) throws Exception{
        Path path = Paths.get(dir);
        System.out.println("watching "+path.toAbsolutePath().toString());

        FileSystem fs = path.getFileSystem();

        try(WatchService service = fs.newWatchService()) {

            // We register the path to the service
            // We watch for creation events
            path.register(service, ENTRY_MODIFY, ENTRY_CREATE);

            // Start the infinite polling loop
            WatchKey key = null;
            while(true) {
                key = service.take();

                // Dequeueing events
                WatchEvent.Kind<?> kind = null;
                for(WatchEvent<?> watchEvent : key.pollEvents()) {
                    // Get the type of the event
                    kind = watchEvent.kind();
                    if (OVERFLOW == kind) {
                        continue; //loop
                    } else if (ENTRY_MODIFY == kind || ENTRY_CREATE == kind) {
                        // A new Path was created
                        Path newPath = ((WatchEvent<Path>) watchEvent).context();

                        String fname = newPath.toString();
                        System.out.println("File "+fname+" touched");
                        if (fname.endsWith(".log.gz"))
                            decompressGzipFile(fname);
                        else if (fname.endsWith(".log"))
                            log_event(fname, dir);
                    }
                }

                if(!key.reset()) {
                    break; //loop
                }
            }

        } catch(IOException ioe) {
            ioe.printStackTrace();
        } catch(InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{
        watch(args[0]);
    }


    private static void decompressGzipFile(String compressedFile) throws Exception{
        Process p = Runtime.getRuntime().exec("tar -xf "+compressedFile);
        p.waitFor();
    }


}
