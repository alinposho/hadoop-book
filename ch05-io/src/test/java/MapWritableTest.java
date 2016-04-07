// == MapWritableTest

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.junit.Test;

public class MapWritableTest extends WritableTestBase {

    @Test
    public void mapWritable() throws IOException {
        // vv MapWritableTest
        MapWritable src = new MapWritable();
        src.put(new IntWritable(1), new Text("cat"));
        src.put(new VIntWritable(2), new LongWritable(163));


        MapWritable dest = new MapWritable();
        WritableUtils.cloneInto(dest, src);
        assertThat(dest.get(new IntWritable(1)), is(new Text("cat")));
        assertThat(dest.get(new VIntWritable(2)), is(new LongWritable(163)));
        // ^^ MapWritableTest
    }

    @Test
    public void using_mutable_keys_in_map_will_cause_values_to_go_missing() {
        // Prepare
        MapWritable src = new MapWritable();
        IntWritable mutable_key = new IntWritable(1);
        src.put(mutable_key, new Text("cat"));

        // Exercise
        mutable_key.set(7);

        // Verify
        assertThat(src.get(new IntWritable(1)), is(nullValue()));
    }

    @Test
    public void setWritableEmulation() throws IOException {
        MapWritable src = new MapWritable();
        src.put(new IntWritable(1), NullWritable.get());
        src.put(new IntWritable(2), NullWritable.get());

        MapWritable dest = new MapWritable();
        WritableUtils.cloneInto(dest, src);
        assertThat(dest.containsKey(new IntWritable(1)), is(true));
    }
}
