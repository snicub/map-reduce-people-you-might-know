import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class PeopleYouMightKnowMapReduce {

    // mapper for calculatng mutual friends between users.
    public static class MutualFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        //mapping function to parse over the input 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //split each input line into user and their friends list.
            String[] userAndFriends = value.toString().split("\t");
            
            String user = userAndFriends[0];
            //check if the user has friends, split the friends list if present
            String[] friends = userAndFriends.length > 1 ? userAndFriends[1].split(",") : new String[0];
    
            //take uout  pairs of potential connections and direct friendships
            for (String friend : friends) {
                //sort d combine user and friend to ensure consistency in key
                String userPair = getSortedPair(user, friend);
                //check and indicates that these two are already direct friends.
                context.write(new Text(userPair), new Text("#"));
    
                // emit pairs of friends for potential mutual connections
                for (String mutualFriend : friends) {
                    // avoid takign our  a pair where the friend is compared to themselves
                    if (!friend.equals(mutualFriend)) {
                        String mutualPair = getSortedPair(friend, mutualFriend);
                        // emit potential mutual friends along with the user
                        context.write(new Text(mutualPair), new Text(user));
                    }
                }
            }
        }
    
        //heelllper method to get sorted pair of users to ensure consistency in map output keys
        private String getSortedPair(String user1, String user2) {
            //use lexicographic order to sort user1 and user2
            return user1.compareTo(user2) < 0 ? user1 + "," + user2 : user2 + "," + user1;
        }
    }

    // educer for finding mutual friends and filtering out direct friends
    public static class MutualFriendsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // a set to store the mutual friends
            Set<String> mutualFriends = new HashSet<>();
            boolean isDirectFriend = false;

            // iterate through all the values associated with a user pair
            for (Text value : values) {
                // '#' indicates direct friendship between the users
                if (value.toString().equals("#")) {
                    isDirectFriend = true;
                } else {
                    // add non-direct friends to the mutualFriends set
                    mutualFriends.add(value.toString());
                }
            }

            // if the users are already friends, no recommendation should be made
            if (!isDirectFriend) {
                // split the user pair into individual users
                String[] users = key.toString().split(",");
                String user1 = users[0];
                String user2 = users[1];
                int mutualCount = mutualFriends.size();

                // emit the number of mutual friends for each user pair
                context.write(new Text(user1), new Text(user2 + ":" + mutualCount));

                context.write(new Text(user2), new Text(user1 + ":" + mutualCount));
            }
        }
    }

    //rrreeducer for generating the final friend recomendations for each user
    public static class RecommendationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //map to store potential recommendations with the number of mutual friends
            Map<String, Integer> recommendations = new HashMap<>();

            // iterate through the mutual friend counts for each user
            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts.length == 2) {
                    // extract the recommended user's id and mutual friend count
                    String recommendedUser = parts[0];
                    int mutualFriendCount = Integer.parseInt(parts[1]);
                    recommendations.put(recommendedUser, mutualFriendCount);
                }
            }

            // sort recomendations based on mutual friend count (descending), then user ID (ascending)
            List<Map.Entry<String, Integer>> sortedRecommendations = new ArrayList<>(recommendations.entrySet());
            sortedRecommendations.sort((a, b) -> {
                int cmp = b.getValue().compareTo(a.getValue());
                return cmp == 0 ? a.getKey().compareTo(b.getKey()) : cmp;
            });

            // get the top 10 recommended users based on mutual friends
            List<String> topRecommendations = new ArrayList<>();
            for (int i = 0; i < Math.min(10, sortedRecommendations.size()); i++) {
                topRecommendations.add(sortedRecommendations.get(i).getKey());
            }

            // emit the sorted top 10 recommendations for each user
            context.write(key, new Text(String.join(",", topRecommendations)));
        }
    }

    // identity mapper to pass intermediate results to the final reducer
    public static class IdentityMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // split input line into user and their recommendations.
            String[] userAndRecommendations = value.toString().split("\t");
            if (userAndRecommendations.length == 2) {
                // pass the data directly to the reducer.
                context.write(new Text(userAndRecommendations[0]), new Text(userAndRecommendations[1]));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // stage 1: calculates mutual friends between users
        Job job1 = Job.getInstance(conf, "Mutual Friends Calculation");
        job1.setJarByClass(PeopleYouMightKnowMapReduce.class);
        job1.setMapperClass(MutualFriendsMapper.class);
        job1.setReducerClass(MutualFriendsReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // specify input and output paths for stage 1
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path intermediateOutput = new Path("intermediate_output");
        FileOutputFormat.setOutputPath(job1, intermediateOutput);

        // delete intermediate output if it exists to avoid conflictsss
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(intermediateOutput)) {
            fs.delete(intermediateOutput, true);
        }

        // run job1 and check for successs
        boolean success = job1.waitForCompletion(true);
        if (!success) {
            System.exit(1);
        }

        // stage 2: generates final friend recommendation
        Job job2 = Job.getInstance(conf, "Recommendation Generation");
        job2.setJarByClass(PeopleYouMightKnowMapReduce.class);
        job2.setMapperClass(IdentityMapper.class);
        job2.setReducerClass(RecommendationReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, intermediateOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        // delete final output if it exists to prevent errors and unexpcte dmathces
        Path finalOutput = new Path(args[1]);
        if (fs.exists(finalOutput)) {
            fs.delete(finalOutput, true);
        }

        // run job2 and exit based on success or failure on boean
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
