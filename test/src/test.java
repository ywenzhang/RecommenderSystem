import java.io.*;
import java.util.HashSet;

/**
 * Represent test with their details-- . *
 *
 * @author Yiwen Zhang
 */
public class test {
  public static void main(String[] args){
    HashSet<String> movies= new HashSet<String>();
    try {
      File file = new File("topmovie_ratings.csv");    //creates a new file instance
      FileReader fr = new FileReader(file);   //reads the file
      BufferedReader br = new BufferedReader(fr);  //creates a buffering character input stream
      String line;
      while ((line = br.readLine()) != null) {
        String[] info = line.split(",");
        movies.add(info[1]);
      }
      fr.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println(movies.size());
  }
}
