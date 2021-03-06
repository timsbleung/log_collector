/**
 * Created by tl on 2/9/15.
 */

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import static java.nio.file.StandardCopyOption.*;
import java.security.MessageDigest;

public class brolog_parser extends log_parser{

    private static final String separator_keyword = "#set_separator";
    private static final String fields_keyword = "#fields";

    int parse_log(String path, List<String> config, int starting_line) throws Exception {
        File config_file = new File(path);
        List<String> lines = new ArrayList<String>();
        try {
            lines = Files.readAllLines(config_file.toPath(), Charset.defaultCharset());
        } catch(Exception e) {
            throw new Exception("invalid log file path - "+path);
        }
        String separator = "\t";
        List<Integer> indices = get_fieldlist_indices(lines, separator, config);
        //System.out.println("processing log...");
        int i = 0;
        for (i=starting_line; i<lines.size(); i++) {
            String line = lines.get(i);
            if (line.length()==0 || line.charAt(0)=='#')
                continue;
            append_line(line.split(separator), indices, config);
        }
        //System.out.println("DONE");
        return i;
        //put column names on top
    }

    private void append_line(String[] line, List<Integer> indices, List<String> config) throws Exception {
		if (!line[0].equals("close")) {
			MessageDigest hasher;
			try {
				hasher = MessageDigest.getInstance("MD5");
			}
			catch (Exception e) {
				System.out.println("unable to get hasher "+e);
				return;
			}
			log_packet entry = new log_packet();
			int config_idx = 0;
            try {
				    //TODO NOT MAGIC NUMBERS
                    entry.timestamp = line[0];
					entry.event = line[10];
					entry.source = "BRO";
					entry.user = line[2] + ":" + line[3];
					entry.eventid = hasher.digest((entry.timestamp + entry.event + entry.user).getBytes()).toString();

            }
            catch (Exception e) {
                throw new Exception("unable to find separator");
			}
			for (int index : indices) {
				try {
					entry.metadata.put(config.get(config_idx++), line[index]);
				}
				catch (Exception e) {
                    throw new Exception("unable to find separator");
				}
			}
			logs.add(entry);
		}
    }

    private static String get_separator(List<String> lines) throws Exception {
        for (String line : lines) {
            if (line.contains(separator_keyword))
                return line.substring(separator_keyword.length());
        }
        throw new Exception("unable to find separator");
    }

    private static List<Integer> get_fieldlist_indices(List<String> lines, String separator, List<String> config) throws Exception{
        for (String line : lines) {
            if (line.contains(fields_keyword)) {
                List<String> fields = Arrays.asList(line.substring(fields_keyword.length()+separator.length()).split(separator));
                return get_indices(fields, config);
            }
        }
        throw new Exception("unable to find fields list");
    }

    public static List<Integer> get_indices(List<String> fields, List<String> config) {
        List<Integer> indices = new ArrayList<Integer>();
        for (int i=0; i<fields.size(); i++) {
            String field = fields.get(i);
            if (config.contains(field))
                indices.add(i);
        }
        return indices;
    }

    public void update_conf_file(File file, int new_idx) {
        PrintStream out = null;
        try {
            String new_conf = new String(Files.readAllBytes(Paths.get(file.getAbsolutePath())));
            new_conf = new_conf.substring(0, new_conf.indexOf("\n"))+"\n"+new_idx;
            out = new PrintStream(new FileOutputStream(file.getAbsoluteFile()));
            out.print(new_conf);
        } catch (IOException e) {
            System.out.println("unable to update configuration file");
            e.printStackTrace();
        }
        finally {
            if (out!=null)
                out.close();
        }
    }

    public List<String> get_config_strings(List<String> config_lines) throws Exception{
        List<String> config = Arrays.asList(config_lines.get(0).split("\t"));
        return config;
    }

    public int get_starting_line(List<String> config_lines) throws Exception {
        return Integer.parseInt(config_lines.get(1));
    }


    public void generate_config_file(File file) throws Exception{
        File template = new File("./conf/templates/bro_template.conf");
        Files.copy(template.toPath(), file.toPath(), REPLACE_EXISTING);
    }


    //take config and log file as parameter to command line
}
