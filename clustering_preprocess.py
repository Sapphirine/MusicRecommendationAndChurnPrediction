user = {}
with open('/Users/yiming/Documents/BigData/final_project/recommeation/input/members.csv', 'r') as f:
    lines = f.readlines()
    for line in lines:
        user[line.split(',')[0]] = {}

song = {}
genre_set = set()
with open('/Users/yiming/Documents/BigData/final_project/recommeation/input/songs.csv', 'r') as f:
    lines = f.readlines()
    for line in lines:
        curr_line = line.split(',')
        song_id = curr_line[0]
        genre_ids = curr_line[2].split('|')
        for genre_id in genre_ids:
            genre_set.add(genre_id)
        song[song_id] = genre_ids

for u in user:
    for genre in genre_set:
        user[u][genre] = 0
    if "" in user[u]:
        user[u].pop("")

with open('/Users/yiming/Documents/BigData/final_project/recommeation/input/train.csv', 'r') as f:
    lines = f.readlines()
    for line in lines:
        curr_line = line.split(',')
        user_id = curr_line[0]
        song_id = curr_line[1]
        if song_id in song and user_id in user:
            for genre_id in song[song_id]:
                if genre_id != "":
                    user[user_id][genre_id] += 1

print user['ed0hCzvhKVxHMIodLtgkzb1G4ryiet50rO/GE3W3pEA=']

with open('/Users/yiming/PyCharmProjects/BigData_final/dataset_for_cluster.csv', 'w') as f:
    for u in user:
        for genre in user[u]:
            # print user[u], ','
            f.write(str(user[u][genre]))
            f.write(',')
        f.write(u)
        f.write(',\n')