import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represent CompositeGroupKey with their details-- . *
 *
 * @author Yiwen Zhang
 */
public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
  String movieId;
  String order;
  String times;
  public CompositeGroupKey() {
  }
  public CompositeGroupKey(String movieId, String order, String times) {
    super();
    this.movieId = movieId;
    this.order = order;
    this.times = times;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(movieId);
    out.writeUTF(order);
    out.writeUTF(times);
  }

  public void readFields(DataInput in) throws IOException {
    movieId = in.readUTF();
    order = in.readUTF();
    times = in.readUTF();
  }

  public int compareTo(CompositeGroupKey key) {
    if (key == null) {
      return 0;
    }
    int res = this.movieId.compareTo(key.movieId);
    if(res == 0){
      res = this.order.compareTo(key.order);
    }
    return res;
  }
}