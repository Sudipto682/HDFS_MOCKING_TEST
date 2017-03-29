import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.nio.ch.IOUtil;

import java.io.*;

/**
 * Created by ssah22 on 3/24/2017.
 */
public class DFSCluster {

    private MiniDFSCluster cluster;
    private String hdfsURI;

    @Before
    public void setUp() throws Exception {
        // super.setUp();
        Configuration conf = new Configuration();
        File baseDir = new File("./target/hdfs/").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1048576);

        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        cluster = builder.numDataNodes(2).build();

        hdfsURI = "hdfs://localhost:"+ cluster.getNameNodePort() + "/";
        System.out.println("Cluster UP ----->"+cluster.isClusterUp());
        System.out.println("HDFS URI ------> "+hdfsURI);

        System.out.println("Count od number of data nodes ----> "+cluster.getDataNodes().size());
        FileSystem fs = null;
        try{
            fs = cluster.getFileSystem();
            System.out.println("Hdfs Path exists --->"+fs.exists(new Path(hdfsURI.replace("localhost", "127.0.0.1")+"dir1")));
            fs.mkdirs(new Path(hdfsURI.replace("localhost", "127.0.0.1")+"dir1"));
            System.out.println("Local Path exists --->"+fs.isFile(new Path("~/test.txt")));
            //System.out.println("HOME PAth exisits --->"+fs.exists(new Path("file:///C:/")));

            FileSystem fsLocal = FileSystem.getLocal(conf).getRawFileSystem();
            fsLocal.createNewFile(new Path("file:///C:/Users/sgu197/workspace/MiniDFSApp/test10.txt"));
            FileOutputStream out =new FileOutputStream("/C:/Users/sgu197/workspace/MiniDFSApp/test10.txt");
            String s="Hi This is Sudipto Saha";
            out.write(s.getBytes());
            out.close();
            //fsLocal.copyFromLocalFile(new Path("file:///C:/Users/sgu197/workspace/MiniDFSApp/abcxyz.txt"),new Path(hdfsURI.replace("localhost", "127.0.0.1")+"dir1/"));
            fs.copyFromLocalFile(new Path("file:///C:/Users/sgu197/workspace/MiniDFSApp/test10.txt"),new Path(hdfsURI.replace("localhost", "127.0.0.1")+"dir1/"));
        }
        catch(Exception ex){
            System.out.println("CAught exception ------>");
            ex.printStackTrace();

        }
        System.out.println("ending -------->");
        System.out.println("The file is there on hadoop ------> "+fs.isFile(new Path(hdfsURI.replace("localhost", "127.0.0.1")+"dir1/test10.txt")));

        //trying to read the file pushed in hdfs
        String uri="hdfs://127.0.0.1:"+cluster.getNameNodePort()+"/dir1/test10.txt";
        InputStream in=null;
        in=fs.open(new Path(uri));
        IOUtils.copyBytes(in,System.out,4096,false);

        //check the status information of the file

        String dir_uri="hdfs://127.0.0.1:"+cluster.getNameNodePort()+"/dir1/test10.txt";
        FileStatus stat=fs.getFileStatus(new Path(dir_uri));
        System.out.println("File permission ---"+stat.getPermission());
        System.out.println("File owner ---"+stat.getOwner());
        System.out.println("File path ----"+stat.getPath());

        //checking a directory for any file present(ls)

        String ls_uri="hdfs://127.0.0.1:"+cluster.getNameNodePort()+"/dir1/";
        FileStatus[] status=fs.listStatus(new Path(ls_uri));
        Path[] listedPath=FileUtil.stat2Paths(status);
        for(Path p:listedPath)
        {
            System.out.println(p);
        }
    }

    @Test
    public void method1(){

    }

    @After
    public void tearDown() throws Exception {
        cluster.shutdown();
        System.out.println("Cluster UP --->"+cluster.isClusterUp());
    }


    /*public void testCreateLogEntry() throws Exception {
        //String logentry = new LogEntry().createLogEntry("TestStage", "TestCategory", "/testpath", cluster.getFileSystem());
        String date = new SimpleDateFormat("yyyyMMdd").format(new Date());
        //assertTrue(logentry.startsWith(String.format("/testpath/TestStage_%s_", date)));
    }*/

}
