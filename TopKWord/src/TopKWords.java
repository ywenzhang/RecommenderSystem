import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Represent TopKWord with their details-- . *
 *
 * @author Yiwen Zhang
 */

class WordCountPair{
  String word;
  Long count;
  public WordCountPair(String word, Long count){
    this.word = word;
    this.count = count;
  }

  public String getWord() {
    return word;
  }

  public Long getCount() {
    return count;
  }
}

class pairComparator implements Comparator<WordCountPair> {
  public int compare(WordCountPair p1, WordCountPair p2)
  {
    if(p1.getCount()<p2.getCount()){
      return 1;
    }
    else if(p1.getCount()>p2.getCount()){
      return -1;
    }
    return 0;
  }
}

public class TopKWords {

  Map<String,Long> wordCountMap;
  String filePath;
  Set<String> temp;
  Queue<WordCountPair> pQueue;

  public TopKWords(String filePath){
    this.wordCountMap = new HashMap<>();
    this.filePath = filePath;
  }


  public void textPreProcessing() throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(filePath));
    String line = reader.readLine();
    while (line != null){
      // change all letters to lower case
      String toLower = line.toLowerCase();
      // replace all special characters
      String replaceSpecial = toLower.replaceAll("[^a-zA-Z0-9]", " ");
      String[] words = replaceSpecial.split("\\s+");
      for(String word:words){
        wordCountMap.put(word,wordCountMap.getOrDefault(word,new Long(0))+1);
      }
      line = reader.readLine();
    }
  }

  public Set<String> getTopKWords(Integer k, Set<String> wordsToExclude){

    if(this.temp == null || k < temp.size()) {
      //Using a max heap to record all top k words
      this.pQueue = new PriorityQueue<>(new pairComparator());
      for (String word : wordCountMap.keySet()) {
        if (!wordsToExclude.contains(word)) {
            pQueue.add(new WordCountPair(word, wordCountMap.get(word)));
        }
      }
      Set<String> topK = new HashSet<>();
      while(pQueue.size()!=0 && topK.size()<k){
        String word = pQueue.poll().getWord();
        topK.add(word);
      }
      this.temp = topK;
      return topK;
    }else{
      for(String word:wordsToExclude){
        if(this.temp.contains(word)){
          this.temp.remove(word);
          while(this.pQueue.size()>0){
            String candidate = this.pQueue.poll().getWord();
            if(!wordsToExclude.contains(candidate)){
              this.temp.add(candidate);
            }
          }
        }
      }
      while(this.temp.size()<k && this.pQueue.size() > 0){
        String candidate = this.pQueue.poll().getWord();
        if(!wordsToExclude.contains(candidate)){
          this.temp.add(candidate);
        }
      }
      return this.temp;
    }
  }

  public static void main(String[] args) throws IOException {
    String path = args[0];
    TopKWords topKWords = new TopKWords(path);
    topKWords.textPreProcessing();
    Set wordsToExclude = new HashSet();
    wordsToExclude.add("a");
    wordsToExclude.add("an");
    wordsToExclude.add("the");
    System.out.println(topKWords.getTopKWords(5, wordsToExclude));
  }
}
