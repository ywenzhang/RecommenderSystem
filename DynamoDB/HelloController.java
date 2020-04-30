import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.File;
import java.util.*;


@Controller
@EnableAutoConfiguration
public class HelloController {

    private static Map<Integer,Set<Integer>> top1000 = new HashMap<>();

    @RequestMapping("/movie")
    String home() {
        return "search";
    }

    @RequestMapping("/data")
    @ResponseBody
    public List<Map<String,String>> greeting(@RequestParam(value = "id", required = false) Long id) {

        AmazonDynamoDB client = AmazonDynamoDBAsyncClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("Top2");
        HashMap<String, String> nameMap = new HashMap<String, String>();
        nameMap.put("#yr", "user");
        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":id", id);
        QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#yr = :id").withNameMap(nameMap)
                .withValueMap(valueMap);

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;
        List<Integer> list = new LinkedList<>();
        Map<String,Double> rate = new HashMap<>();
        try {
            System.out.println("Try to find the data");
            items = table.query(querySpec);
            iterator = items.iterator();
            while (iterator.hasNext()) {
                System.out.println("Check if user has watched this movie");
                Item ite = iterator.next();
                int n = ite.getInt("movie");
                double r = ite.getDouble("rate");
                System.out.println("Find a anwser");
                if (!top1000.containsKey(id) || !top1000.get(id).contains(n)) {
                    list.add(n);
                    rate.put(id+""+n,r);
                }
            }
        }
        catch (Exception e) {
            System.err.println("Something wrong");
            System.err.println(e.getMessage());
        }
        List<Map<String,String>> result = new ArrayList<>();
        for (int index=list.size()-1;index>=0;index--){
            System.out.println("Check the result");
            int i = list.get(index);
            Table t = dynamoDB.getTable("Movie");
            HashMap<String, String> name = new HashMap<String, String>();
            name.put("#yr", "id");
            HashMap<String, Object> value = new HashMap<String, Object>();
            value.put(":id", i);
            QuerySpec query = new QuerySpec().withKeyConditionExpression("#yr = :id").withNameMap(name)
                    .withValueMap(value);
            try {
                ItemCollection<QueryOutcome> its = t.query(query);
                Iterator<Item> iter = its.iterator();
                while (iter.hasNext()) {
                    Item it = iter.next();
                    Map<String,String> map = new HashMap<>();
                    map.put("title",it.getString("titile"));
                    map.put("description",it.getString("description"));
                    String hp = it.getString("homepage");
                    map.put("homepage",hp.equals("Null")?"Not Available":hp);
                    String bg = it.getString("budget");
                    map.put("budget",bg.equals("0")?"Not Available":bg);
                    map.put("rate",rate.get(id+""+i)+"");
                    result.add(map);
                    System.out.println("Find a result "+i);
                    if (result.size()>4) break;
                }
                if (result.size()>4) break;
            }

            catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
        System.out.println(result);
        return result;
    }

    public static void main(String[] args) throws Exception {
        File file = new File("src/1000UserRating.csv");
        Scanner scan = new Scanner(file);
        while (scan.hasNextLine()){
            try {
                String[] s = scan.nextLine().split(",");
                int user = Integer.parseInt(s[0]);
                int movie = Integer.parseInt(s[1]);
                top1000.putIfAbsent(user, new HashSet<>());
                top1000.get(user).add(movie);
            }catch (Exception e){
                continue;
            }
        }
        SpringApplication.run(HelloController.class, args);
        //运行之后在浏览器中访问：http://localhost:8080/movie
    }

}