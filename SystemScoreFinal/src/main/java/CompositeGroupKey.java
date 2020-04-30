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
  String user;

  public CompositeGroupKey() {
  }
  public CompositeGroupKey(String movieId1, String user) {
    super();
    this.movieId = movieId1;
    this.user = user;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(movieId);
    out.writeUTF(user);
  }

  public void readFields(DataInput in) throws IOException {
    movieId = in.readUTF();
    user = in.readUTF();
  }

  public int compareTo(CompositeGroupKey key) {
    if (key == null) {
      return 0;
    }
    int res = this.movieId.compareTo(key.movieId);
    if(res == 0){
      res = this.user.compareTo(key.user);
    }
    return res;
  }
  @Override
  public String toString(){
    return   movieId + "," + user;
  }
}