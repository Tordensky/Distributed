package motion;
 
import java.io.IOException;
import java.io.InputStream;
 
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
 
public class TestInpForm extends org.apache.hadoop.mapred.FileInputFormat<Text, Text> {
 
  @Override
  public RecordReader<Text, Text> getRecordReader(InputSplit arg0, JobConf arg1,
      Reporter arg2) throws IOException {
    final InputStream in = FileSystem.get(arg1).open(((FileSplit)arg0).getPath());
    final FileStatus stat = FileSystem.get(arg1).getFileStatus(((FileSplit)arg0).getPath());
    return new RecordReader<Text, Text>() {
 
      private byte[] bigbytearray = new byte[10*1024*1024]; // 10 MB byte array.
      private boolean unread = true;
 
      @Override
      public void close() throws IOException {
        in.close();
      }
 
      @Override
      public Text createKey() {
        return new Text();
      }
 
      @Override
      public Text createValue() {
        return new Text();
      }
 
      @Override
      public long getPos() throws IOException {
        return unread ? 0 : stat.getLen();
      }
 
      @Override
      public float getProgress() throws IOException {
        return unread ? 0 : 1;
      }
 
      @Override
      public boolean next(Text arg0, Text arg1) throws IOException {
        if (unread) {
          IOUtils.readFully(in, bigbytearray , 0, (int) stat.getLen());
          arg1.set(bigbytearray);
          arg0.set(stat.getPath().toString().getBytes());
          unread  = false;
          return true;
        } else {
          return false;
        }
      }
    };
  }
  
  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }
 
}