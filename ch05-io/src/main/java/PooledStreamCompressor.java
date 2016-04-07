// cc PooledStreamCompressor A program to compress data read from standard input and write it to standard output using a pooled compressor

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.function.Function;

// vv PooledStreamCompressor
public class PooledStreamCompressor {

    public static void main(String[] args) throws Exception {
        String codecClassname = args[0];
        Class<?> codecClass = Class.forName(codecClassname);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        withCompressor(codec, compressor -> {
            CompressionOutputStream out = codec.createOutputStream(System.out, /*[*/compressor/*]*/);
            IOUtils.copyBytes(System.in, out, 4096, false);
            out.finish();
            return true;
        });
    }

    private static <T> T withCompressor(CompressionCodec codec, CheckedFunction<Compressor, T> f) throws IOException {
        Compressor compressor = null;
        try {
            compressor = CodecPool.getCompressor(codec);
            return f.apply(compressor);
        } finally {
            CodecPool.returnCompressor(compressor);
        }
    }

    @FunctionalInterface
    public interface CheckedFunction<T, R> {
        R apply(T t) throws IOException;
    }
}
// ^^ PooledStreamCompressor
