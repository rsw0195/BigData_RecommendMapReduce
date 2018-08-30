# Execution Statement:
# !python rec-f.py input-f.txt > friends.txt
# Note, this took about 5 minutes to run on Dr. Wilck's work CPU

# Used two mappers and two reducers in this python mapreduce program.
# In the first Mapper, 
# (1):Reads each line to generate friend pairs;Parameters: key- friend_pair 
# (2):If already friend, value = 0;
# (3):If 1 common friend, value = 1;

# In the first Reducer:
# (1):Count total common friends for each key 
# (2):Parameters: key - friend_pair, value ¨C sum(value)
                         
# In the second Mapper
# (1):Use user as key to map the value pairs
# (2):Parameters: key ¨C user, value ¨C friends, sum(value)

# In the second Reducer
# (1):For each user, find the most 10 common friend recommendations
# (2):Parameters: key ¨C user, value ¨C the most 10 common friend recommendations

from mrjob.job import MRJob
from mrjob.step import MRStep

class FriRec(MRJob):

    def steps(self):
        return  [
            MRStep(mapper = self.mapper1,
                   reducer = self.reducer1),
            MRStep(mapper = self.mapper2, 
                   reducer = self.reducer2)
            ]
    
    def mapper1(self, _, line):
        user_id, friends = line.split("\t")
        friend_ids = friends.split(",")
        
        for friend in friend_ids:
            yield (user_id + "," + friends, 0)
        for friend_i in friend_ids:
            for friend_j in friend_ids:
                if friend_i == friend_j:
                    continue
                yield (friend_i + "," + friend_j, 1)
    def reducer1(self, friend_pair, values):
        if not( 0 in values):
            yield (friend_pair, sum(values))
            
    def mapper2(self, friend_pair, value_sum):
        user_id, friend_id = friend_pair.split(",")
        yield (user_id, friend_id + "," + str(value_sum))
    
    def reducer2(self, user_id, values):
        all_friends = []
        for value in values:
            friend_id, value_sum = value.split(",")
            friend_id = int(friend_id)
            value_sum = int(value_sum)
            all_friends.append((value_sum, friend_id))
            
        # find the most 10 common friend recommendations
        most_common_friends = sorted(all_friends, reverse = True)
        if len(most_common_friends) > 10:
            most_common_friends = most_common_friends[:10]
    
        # buidling friends string, separated by comma.
        friend_id_strs = []
        for fri in most_common_friends:
            friend_id_strs.append(str(fri[1]))
        yield (user_id, ",".join(friend_id_strs))

if __name__ == '__main__':
    FriRec.run()