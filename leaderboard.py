from quart import Quart, request
from quart_schema import QuartSchema
import redis
import httpx
import os
import socket
import time

has_run = False


#register leaderboard service with game service
def register_callback(num = 0):
    global has_run
    print("VALUE IS",has_run)
    if has_run :
        return
    if num > 12:
        raise Exception('Could not connect nuke foreman with exception')
    try:
        #construct callback url for leaderboard service
        url = socket.getfqdn("http://localhost")
        port = os.environ.get('PORT')

        #register callback url to games service
        payload = {"url":f"{url}:{port}/postgame"}
        resp = httpx.post(f'http://localhost:5100/addurl',json=payload)
        has_run = True
        resp.raise_for_status()
    except (ConnectionError, httpx.HTTPError):
        time.sleep(10)
        register_callback(num + 1)
    

app = Quart(__name__)
register_callback()
QuartSchema(app)

@app.route('/postgame', methods=['POST'])
async def postgame():
    r = redis.Redis(db=0)
    data = await request.get_json()

    #data from game service 
    username = data["username"]
    score = data["score"]

    r.lpush(username,score) #push user and score as a list
    r.zadd("username",{username:score}) # will be used to traverse all users

    json_result = {}
    #iterate all users and combine their individual scores and add it using zadd
    for key in r.zrange("username",0,-1):
        score_list = r.lrange(key,0,-1)
        avg_score = 0
        #loop through all scores for user, val is in bytes
        for val in score_list:
            avg_score += int(val) 
        #calc avg by total score / len of games
        avg_score = int(avg_score/r.llen(key))
        json_result[key.decode('utf-8')] = avg_score
        r.zadd("username",{key.decode('utf-8'): avg_score})
    return json_result

@app.route('/leaderboard')
async def leaderboard():
    r = redis.Redis(db=0)
    # r.flushdb() #used to clear db

    url = socket.getfqdn("http://localhost")
    port = os.environ.get('PORT')

    #register callback url to games service
    payload = {"url":f"{url}:{port}"}
    resp = httpx.post(f'http://localhost:5100/addurl',json=payload)
    #  get list of users and their scores
    scoreL = r.zrange("username",0,-1,withscores=True)
    result = {}
    #include all users if there is less 10
    if(len(scoreL) < 10 and len(scoreL) >= 1):
        for i in range(0,len(scoreL)):
            #add top 10 from scoreL starting from end, since it is sorted in asc order
            result[scoreL[len(scoreL)-i-1][0].decode('utf-8')] = int(scoreL[len(scoreL)-i-1][1])
    else:
        for i in range(0,10):
            #add top 10 from scoreL starting from end, since it is sorted in asc order
            result[scoreL[len(scoreL)-i-1][0].decode('utf-8')] = int(scoreL[len(scoreL)-i-1][1])
    
    return result

@app.errorhandler(409)
def conflict(e):
    return {"error": str(e)}, 409

