import java.util.PriorityQueue;

/**
 * Represent Test with their details-- . *
 *
 * @author Yiwen Zhang
 */
public class Test {
  public static void main(String[] args){
    PriorityQueue<MovieRatingPair> pq = new PriorityQueue<>(5, new MovieRatingPairComparator());
    pq.add(new MovieRatingPair("1",3.0));
    pq.add(new MovieRatingPair("2",2.1));
    MovieRatingPair pair = pq.poll();
    System.out.println(pair.rating);
  }
}
