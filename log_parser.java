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

    private static final boolean SEND_TO_KAFKA = false;
    private static final boolean OUTPUT_TO_FILE = true;

    //the address that we should query for the kafka server IP address
    public static final String QUERY_ADDRESS = "http://vivaldi.crhc.illinois.edu:4001/v2/keys/services/kafka";

    //a map that contains the columns we want from the log, mapping the column names to values for each entry in the log
    protected List<log_packet> logs;


    /****************************************************************************
     * virtual functions that must be filled out for a parser to be functional
     ***************************************************************************/

    /**
     * The function that is called for the parser to parse the log file
     *
     * @param path to the log file that you wish to be parsed
     * @param config the configuration strings for the parser, aka the information we want filtered
     * @param starting_line the line to start parsing the log at,
     * @return the line that we end parsing at
     * @throws Exception Unable to parse the log file (it is an invalid log file)
     */
    abstract int parse_log(String path, List<String> config, int starting_line) throws Exception;

    /**
     * call this function after the parser has finished, with new_idx as the result of parse_log
     * @param file the path to the configuration file to update
     * @param new_idx the new index we have written to. This is written into the configuration file so that on repeated
     *                writes to the same file, we won't keep reading the same lines
     */
    abstract void update_conf_file(File file, int new_idx);

    /**
     * If we can not find a configuration file for the log file, generate one based on a template defined by the logger
     * @param file Path to the blank log file
     * @throws Exception invalid destination for a config file write
     */
    abstract void generate_config_file(File file) throws Exception;

    /**
     * Given a configuration file, pulls out the parsing related configuration information we need, for instance,
     * the columns we want to have filtered in the log
     *
     * @param config_lines the raw string lines read from the configuration file
     * @return List of columns we want pulled out (but can really be anything you want as long as your parser's
     *          parse_log function expects the same thing.
     * @throws Exception
     */
    abstract List<String> get_config_strings(List<String> config_lines) throws Exception;

    /**
     * tell the parser where to find in the configuration line where to start reading
     * @param config_lines the raw string lines read from the configuration file
     * @return the line to start reading at
     * @throws Exception
     */
    abstract int get_starting_line(List<String> config_lines) throws Exception;



    void output_data() {
        if (OUTPUT_TO_FILE)
            output_to_file();
        if (SEND_TO_KAFKA)
            send_to_kafka();
    }

    /**
     * cURLs a pre determined IP address to get the IP address of the kafka server
     * @return a string containing the IP address of the kafka server
     * @throws Exception, unable to get the address of the Kafka server IP address
     */
    private static String get_kafka_IP() throws Exception{
        URL url = new URL(QUERY_ADDRESS);
        String kafkaIP = "";
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"))) {
            String line = reader.readLine();
            Map<String, Object> values = new Gson().fromJson(line, Map.class);
            kafkaIP = (String )((Map<String, Object>)values.get("node")).get("value");
        }
        return kafkaIP;
    }

    /**
     * A file that currently just prints out the json strings
     */
    void output_to_file() {
        for (log_packet entry : logs) {
            entry.get_string();
        }
    }

    /**
     * A simple function that takes a map<String, String> and spits out the corresponding json string
     * @param map
     * @return a JSON formatted string
     */
    static String map_to_string(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<String, String> entry : map.entrySet())
            sb.append("\"" + entry.getKey() + "\": \"" + entry.getValue() + "\", ");
        sb.deleteCharAt(sb.length()-1); sb.deleteCharAt(sb.length()-1);
        sb.append("}");
        return sb.toString();
    }

    /**
     * send the data to the kafka server
     */
    void send_to_kafka() {
        Properties props = new Properties();
        String kafkaIP = null;
        try {
            kafkaIP = get_kafka_IP();
        } catch (Exception e) {
            System.out.println("unable to get kafka IP address " + e);
            return;
        }
        props.put("bootstrap.servers", kafkaIP);
        Producer<String, String> producer = new KafkaProducer(props, new StringSerializer(), new StringSerializer());


        for (log_packet entry : logs) {
            String key = "";
            String msg = entry.get_string();
            producer.send(new ProducerRecord<String, String>("the-topic", key, msg));
        }
        producer.close();
    }

    /**
     * the event that is triggered when a log file has been modified in the directory
     * Loads all the needed pieces (config strings, line to start parsing at, the actual file) and then passes
     * work off to the parser
     *
     * NOTE: Currently only bro logs are supported and as such it is hard coded to be parsed by a bro parser.
     * Once more parsers are supported some kind of switch/ifelse statement should be put in place to determine
     * what kind of parser should be initialized.
     *
     * @param filename the name of the file that had an event trigged on it (it should be a log file)
     * @param dir the directory where the file resides
     */
    public static void log_event(String filename, String dir) {
        //initialize the parser
        log_parser parser = new brolog_parser();
        parser.logs = new ArrayList<>();
        long start = System.currentTimeMillis();

        //get the configuration file
        String conf_name = dir+"/conf/"+filename.substring(0, filename.indexOf(".log"))+".conf";

        File config_file = new File(conf_name);
        if (!config_file.exists())
            try {
                //if config file for the log file does not exist, generate one in accordance to what parser is being used
                parser.generate_config_file(config_file);
            }
            catch (Exception e) {
                System.out.println(e);
                return;
            }

        //read all the lines in the configuration file
        List<String> config_lines = new ArrayList<String>();
        try {
            config_lines = Files.readAllLines(config_file.toPath(), Charset.defaultCharset());
        } catch(Exception e) {
            System.out.println("Invalid file read for config_strings"+e);
            return;
        }

        //pass config lines to parser for it to determine what kind of things it needs to parse
        List<String> config_strings = null;
        try {
            config_strings = parser.get_config_strings(config_lines);
        } catch (Exception e) {
            System.out.println("Unable to parse config_strings file for strings" + e);
            return;
        }

        //read from config lines what line we should start parsing from
        int starting_line = 0;
        try {
            starting_line = parser.get_starting_line(config_lines);
        } catch (Exception e) {
            System.out.println("Unable to parse config_strings file for starting line" + e);
            return;
        }

        String path = dir+"/"+filename;
        int lines_parsed = 0;

        //once all the needed components are correctly loaded, parse the log using the parser
        try {
            lines_parsed = parser.parse_log(path, config_strings, starting_line);
        }
        catch (Exception e) {
            System.out.println("unable to parse log "+e);
            e.printStackTrace();
        }

        //update teh configuration file to note where you have parsed up to on this iteration of the parsing
        parser.update_conf_file(config_file, lines_parsed);
        parser.output_data();
        System.out.println(System.currentTimeMillis());
    }

    /**
     * watches a directory for changes and acts when files are modified, depending on what files they are
     * if it is a .gz, the file will be decompressed into a .log file in which case the watcher should trigger again
     * if a log file, it is passed off to the log_event function where work will be done to it
     *
     * @param dir the directory to watch
     * @throws Exception if unable to start monitoring the directories
     */
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
                        //System.out.println("File "+fname+" touched");
                        if (fname.endsWith(".log.gz"))
                            decompressGzipFile(fname);
                        else if (fname.endsWith(".log"))
                            log_event(fname, dir); //act on a log file
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


    public static void main(String[] args){
        try {
            watch(args[0]); //args[0]= directory to watch
        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getStackTrace());
        }
    }


    private static void decompressGzipFile(String compressedFile) throws Exception{
        Process p = Runtime.getRuntime().exec("tar -xf "+compressedFile);
        p.waitFor();
    }
}
