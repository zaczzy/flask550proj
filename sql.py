import mysql.connector
import numpy as np
import time, pdb
from numpy import frombuffer, bitwise_xor, uint64
import time
import pdb


def get_movie_type_name(sql):
    query_column = 'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE Table_name LIKE \'movie_oneHot\''
    cur = sql.cursor()
    cur.execute(query_column)
    rez = cur.fetchall()
    result = np.array(rez)
    movie_type = result[:29, 3]
    return movie_type


def encode_one_hot(input_movie_type_list, movie_type):
    a = list('0' * 28)
    for input_movie_type in input_movie_type_list:
        index = np.where(input_movie_type == movie_type)
        if len(index) != 0:
            a[index[0][0]] = '1'
    code = ''.join(a)
    code = 'b\'' + code + '\''
    return code


def find_peopleid(sql, name):
    a = time.time()
    cur = sql.cursor()
    name = name.replace(" ", "").lower()
    select = 'SELECT people_id, primaryProfession FROM people WHERE edit_primaryName = \'' + name + '\''
    cur.execute(select)
    rez = cur.fetchall()
    result = np.array(rez)
    if len(result) != 0:
        return result[0, 0], result[0, 1]
    else:
        select = 'SELECT people_id, primaryProfession FROM people WHERE soundex(edit_primaryName) = soundex(\'' + name + '\')'
        cur.execute(select)
        rez = cur.fetchall()
        result = np.array(rez)
        if len(result) != 0:
            return result[0, 0], result[0, 1]
        return None, None


def find_moviedid(sql, peopleid, prof):
    cur = sql.cursor()
    if prof != 'director':
        prof_query = '(select movie_id as id from act_in where people_id = ' + peopleid + ') as mid'
    else:
        prof_query = '(select movie_id as id from direct_in where people_id = ' + peopleid + ') as mid'
    query = 'select r.movie_id from ' + prof_query + ',ratings r where mid.id = r.movie_id order by averageRate desc limit 5'
    cur.execute(query)
    rez = cur.fetchall()
    result = np.array(rez).flatten()
    return result


def recommend_actor(sql, query2):
    cur = sql.cursor()
    query4 = '(select people_id as pid from act_in as a, ' + query2 + ' where a.movie_id = t1.id) as t3'
    query = 'select t3.pid, COUNT(*) from ' + query4 + ' group by t3.pid order by COUNT(*) desc limit 1'
    cur.execute(query)
    rez = cur.fetchall()
    result = np.array(rez)
    return result


def recommend_actor_like(sql, movie_id):
    basic_query = '(select movie_id as mid from movies where'
    q_string = ''
    for i in range(movie_id.size - 1):
        c_id = movie_id[i]
        q_string += '  movie_id = ' + str(c_id) + ' or'
    q_string += ' movie_id = ' + str(movie_id[movie_id.size - 1]) + ') as mq'
    query4 = '(select a.people_id as pid from act_in as a, ' + basic_query + q_string + ' where a.movie_id = mq.mid) as t3'
    query = 'select t3.pid, COUNT(*) from ' + query4 + ' group by t3.pid order by COUNT(*) desc limit 1'
    cur = sql.cursor()
    cur.execute(query)
    rez = cur.fetchall()
    result = np.array(rez)
    return result


def get_movie_name(sql, movie_id):
    basic_query = 'SELECT primaryTitle FROM movies WHERE'
    q_string = ''
    for i in range(movie_id.size - 1):
        c_id = movie_id[i]
        q_string += '  movie_id = ' + str(c_id) + ' or'
    q_string += ' movie_id = ' + str(movie_id[movie_id.size - 1])
    cur = sql.cursor()
    cur.execute(basic_query + q_string)
    rez = cur.fetchall()
    movie_name = np.array(rez).flatten()
    return_name = [None] * movie_id.size
    if movie_name.size == 0:
        return return_name
    for i in range(movie_id.size):
        cl = None
        if movie_name[i] is not None:
            cl = movie_name[i]
            if cl.size == 0:
                cl = None
            else:
                # cl = movie_name[i].encode('ascii', 'ignore')
                cl = movie_name[i]
        return_name[i] = cl
    return return_name


def get_movie_actor(sql, movie_id):
    basic_query = 'SELECT p.primaryName, a.movie_id FROM act_in AS a, people AS p WHERE a.people_id = p.people_id AND ('
    q_string = ''
    for i in range(movie_id.size - 1):
        c_id = movie_id[i]
        q_string += '  movie_id = ' + str(c_id) + ' or'
    q_string += ' movie_id = ' + str(movie_id[movie_id.size - 1]) + ')'
    cur = sql.cursor()
    cur.execute(basic_query + q_string)
    rez = cur.fetchall()
    actor_result = np.array(rez)
    basic_query = 'SELECT p.primaryName, a.movie_id FROM direct_in AS a, people AS p WHERE a.people_id = p.people_id AND ('
    cur.execute(basic_query + q_string)
    rez = cur.fetchall()
    director_result = np.array(rez)
    return_a = [None] * movie_id.size
    return_d = [None] * movie_id.size

    if actor_result.size == 0:
        a_idx = None
    else:
        a_idx = np.array(actor_result[:, 1]).astype(np.int64)
    if director_result.size == 0:
        d_idx = None
    else:
        d_idx = np.array(director_result[:, 1]).astype(np.int64)
    for i in range(movie_id.size):
        cl = None
        if a_idx is not None:
            logical = a_idx == movie_id[i]
            cl = actor_result[logical, 0]
            if cl.size == 0:
                cl = None
            else:
                # cl = [ele.encode('ascii', 'ignore') for ele in cl.tolist()]
                cl = cl.tolist()
        return_a[i] = cl
        cl = None
        if d_idx is not None:
            logical = d_idx == movie_id[i]
            cl = director_result[logical, 0]
            if cl.size == 0:
                cl = None
            else:
                # cl = [ele.encode('ascii', 'ignore') for ele in cl.tolist()]
                cl = cl.tolist()
        return_d[i] = cl
    return return_a, return_d


def get_runtime_genre(sql, movie_id):
    return_list = [None] * movie_id.size
    basic_query = 'SELECT primaryTitle, runtimeMinutes ,genres FROM movies WHERE'
    q_string = ''
    for i in range(movie_id.size - 1):
        c_id = movie_id[i]
        q_string += '  movie_id = ' + str(c_id) + ' or'
    q_string += ' movie_id = ' + str(movie_id[movie_id.size - 1])
    cur = sql.cursor()
    cur.execute(basic_query + q_string)
    output_array = np.array(cur.fetchall())
    # cur.execute(basic_query + q_string)
    for i in range(output_array.shape[0]):
        inner_list = [None] * 3
        output = output_array[i]
        inner_list[0] = output[0]
        inner_list[1] = output[1]
        inner_list[2] = output[2]
        return_list[i] = inner_list
    return return_list


def get_rating(sql, movie_id):
    return_list = [None] * movie_id.size
    basic_query = 'SELECT averageRate, numVotes FROM ratings WHERE'
    q_string = ''
    for i in range(movie_id.size - 1):
        c_id = movie_id[i]
        q_string += '  movie_id = ' + str(c_id) + ' or'
    q_string += ' movie_id = ' + str(movie_id[movie_id.size - 1])
    cur = sql.cursor()
    cur.execute(basic_query + q_string)
    output_array = np.array(cur.fetchall())
    # cur.execute(basic_query + q_string)
    for i in range(output_array.shape[0]):
        inner_list = [None] * 2
        output = output_array[i]
        inner_list[0] = output[0]
        inner_list[1] = output[1]
        return_list[i] = inner_list
    return return_list


def user_query(sql, id):
    query = 'SELECT * FROM people WHERE people_id = ' + str(id)
    cur = sql.cursor()
    cur.execute(query)
    output_array = np.array(cur.fetchall())
    return output_array.tolist()


def recommend_query(input_movie_type_list, sql, movie_type, like=''):
    if len(like) == 0:
        like = '-'
    cur = sql.cursor()
    if like:
        pid, prof = find_peopleid(sql, like)
        result = find_moviedid(sql, pid, prof)
        r_actor_id = None
    else:
        code = encode_one_hot(input_movie_type_list, movie_type)
        query3 = '(select movie_id as id, bit_count(' + code + ' ^ onehot) as distance from movie_oneHotString) as t2'
        query2 = '(select t2.id as id, t2.distance as distance from ' + query3 + ' order by t2.distance desc limit 200) as t1'
        # query2 = 'select * from (select id, score from matching limit 1000 order by score) as select_id,'
        r_actor_id = recommend_actor(sql, query2)
        query = 'select r.movie_id from ' + query2 + ', ' + 'ratings r where t1.id = r.movie_id order by averageRate desc limit 5'
        cur.execute(query)
        rez = cur.fetchall()
        result = np.array(rez).flatten()
        if r_actor_id.size != 0:
            r_actor_id = r_actor_id[0, 0]
        else:
            r_actor_id = None
    actor, director = get_movie_actor(sql, result)

    result_list = get_runtime_genre(sql, result)
    rating_list = get_rating(sql, result)
    for i in range(5):
        cur_sub_list = result_list[i]
        if cur_sub_list is None:
            cur_sub_list = ['-', '-', '-']
        if actor[i] is None or len(actor[i]) == 0:
            cur_sub_list.append('-')
        else:
            string = ''
            for j in range(min(3, len(actor[i]))):
                string += actor[i][j] + ','
            cur_sub_list.append(string)
        if director[i] is None or len(director[i]) == 0:
            cur_sub_list.append('-')
        else:
            string = ''
            for j in range(min(3, len(director[i]))):
                string += director[i][j] + ','
            cur_sub_list.append(string)
        cur_sub_list.append(rating_list[i][0])
        cur_sub_list.append(rating_list[i][1])
        result_list[i] = cur_sub_list
    if r_actor_id is None:
        r_a = [like, '-', '-', '-', '-']
    else:
        r_a = user_query(sql, r_actor_id)
        if len(r_a) == 1 and len(r_a[0]) != 0:
            r_a = r_a[0][1:]
        else:
            r_a = ['-', '-', '-', '-', '-']
    return result_list, r_a, result

# cnx = mysql.connector.connect(user='feedmenews',password='12345678',
# 			      host='mydbfeedmenews.cffkuryafwgu.us-east-1.rds.amazonaws.com',database='news')
# # index = 'CREATE INDEX id_index ON people (people_id) USING HASH'
# # cur = cnx.cursor()
# # cur.execute(index)
# type_name = get_movie_type_name(cnx)
# input_type = np.array(['Action','Comedy'])
# result, r_id, valid = recommend_query(input_type, cnx, type_name, '')
# pdb.set_trace()
# 'James Deem
# data = np.fromiter(cur.fetchall(),count = row_count,dtype=int)
