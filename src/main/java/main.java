



public class main {


    public static void main(String[] args) throws Exception {
        String hadoopPath = "your-hadoop-path";
        hdfs HDFS = new hdfs(hadoopPath );

        HDFS.read();
        //HDFS.write();
        //HDFS.copyFromLocal();

    }
}

