import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;

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


    void output_data() {
        if (OUTPUT_TO_FILE)
            output_to_file();
        if (SEND_TO_KAFKA)
            send_to_kafka();
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
       /* Properties props = new Properties();
        Producer<String, String> producer = new KafkaProducer<String, String>(props, new StringSerializer(), new StringSerializer());

        String[] lines = log.toString().split("\\n");

        for(String msg: lines) {
            String key = "";
            producer.send(new ProducerRecord<String, String>("the-topic", key, msg));
        }
        producer.close();
        */
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
            System.out.println(e.getStackTrace().toString());
        }
        parser.update_conf_file(config_file, new_line);
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
