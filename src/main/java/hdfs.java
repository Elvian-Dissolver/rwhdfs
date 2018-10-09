import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

public class hdfs {
    private static final Logger logger = Logger.getLogger("Main");
    protected String hadoopPath;
    protected Configuration conf;
    protected FileSystem fs;

    public hdfs(String hadoopPath){
        this.hadoopPath = hadoopPath;
        this.init();
    }

    protected void init(){
        conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            fs = FileSystem.get(conf);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void read() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter the file path...");
        String filePath = br.readLine();

        Path path = new Path(filePath);
        //FileSystem fs = path.getFileSystem(conf);

        FSDataInputStream inputStream = fs.open(path);
        System.out.println(inputStream.available());
        String out= IOUtils.toString(inputStream, "UTF-8");
        logger.info(out);
        //fs.close();
    }

    public void write() throws IOException {

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter the file path...");
        String filePath = br.readLine();
        System.out.println("Enter the data...");
        String dataPath = br.readLine();

        Path path = new Path(filePath);
        try{
            //delete file if exist
            if(fs.exists(path)){
                fs.delete(path, true);
            }

            //create file and write content to file
            FSDataOutputStream fos = fs.create(path);
            fos.writeUTF(dataPath);
            fos.close();
        }catch(IOException ex){
            System.out.println(ex.getMessage());
        }
    }

    public void copyFromLocal() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter the localPath...");
        String localPath = br.readLine();
        System.out.println("Enter the hdfsPath...");
        String hdfsPath = br.readLine();

        Path srcPath = new Path(localPath);
        Path destPath = new Path(hdfsPath);

        try{
            //check is path exist
            if(fs.exists(destPath)){
                System.out.println("No such destination " + destPath);
                return;
            }

            fs.copyFromLocalFile(srcPath, destPath);
        }catch(IOException ex){
            System.out.println(ex.getMessage());
        }
    }
}
