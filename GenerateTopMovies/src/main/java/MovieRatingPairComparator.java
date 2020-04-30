import java.util.Comparator;

/**
 * Represent MovieRatingPairComparator with their details-- . *
 *
 * @author Yiwen Zhang
 */
public class MovieRatingPairComparator implements Comparator<MovieRatingPair> {
  public int compare(MovieRatingPair p1, MovieRatingPair p2){
    if(p1.rating < p2.rating){
      return -1;
    }
    else if(p1.rating > p2.rating){
      return 1;
    }
    else{
      return 0;
    }
  }
}
