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
  String movieId1;
  String movieId2;
  public CompositeGroupKey(){

  }
  public CompositeGroupKey(String movieId1, String movieId2) {
    super();
    this.movieId1 = movieId1;
    this.movieId2 = movieId2;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(movieId1);
    out.writeUTF(movieId2);
  }

  public void readFields(DataInput in) throws IOException {
    movieId1 = in.readUTF();
    movieId2 = in.readUTF();
  }

  public int compareTo(CompositeGroupKey key) {
    if (key == null) {
      return 0;
    }
    int res = this.movieId1.compareTo(key.movieId1);
    if(res == 0){
      res = this.movieId2.compareTo(key.movieId2);
    }
    return res;
  }
  @Override
  public String toString(){
    return   movieId1 + "," + movieId2 ;
  }
}