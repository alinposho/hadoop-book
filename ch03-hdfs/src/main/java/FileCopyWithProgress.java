// cc FileCopyWithProgress Copies a local file to a Hadoop filesystem, and shows progress

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

// vv FileCopyWithProgress
public class FileCopyWithProgress {
    public static void main(String[] args) throws Exception {
        String localSrc = args[0];
        String dst = args[1];

        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FSDataOutputStream out = fs.create(new Path(dst), () -> {
            System.out.print(".");
        });
//        out.seek(); // There's no seek since Hadoop allows only appends.

        IOUtils.copyBytes(in, out, 4096, true);
    }
}
// ^^ FileCopyWithProgress
