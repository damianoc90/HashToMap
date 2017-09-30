#Cancemi Damiano
#RUN: "python MapReduce.py test_set_tweets.txt"

from mrjob.job import MRJob
latitude = []
longitude = []
data = []
count = 0
TOFIND = ["breakfast", "launch", "dinner"]

class MRFindLine(MRJob):
    def mapper(self, _, line):
        for tags in TOFIND:
            if tags in line:
                yield TOFIND, line
    def reducer(self, key, values):
        for tweets_line in values:
            tweets_line_split = tweets_line.split("\t")
            with open('/Users/damianocancemi/PycharmProjects/HPC/test_set_users.txt', 'r') as f:
                for users_line in f:
                    users_line_split = users_line.split("\t")
                    if tweets_line_split[0] == users_line_split[0] and "UT:" in users_line_split[1]:
                        global count
                        count += 1
                        coords = users_line_split[1].rstrip().replace("UT: ", "").split(",")
                        data.extend([coords[0] + "," + coords[1]])
                        latitude.append(float(coords[0]))
                        longitude.append(float(coords[1]))
                        break

if __name__ == '__main__':
    MRFindLine.run()

    print "Found",count,"tweets."

    #saving result in file named result.txt
    output = open('result.txt', 'w')
    for item in data:
        output.write("%s\n" % item.encode("utf-8"))
