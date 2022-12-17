from quart import Quart, request
from quart_schema import QuartSchema
import redis
import httpx
import os

app = Quart(__name__)
QuartSchema(app)

@app.route('/postgame', methods=['POST'])
async def postgame():
    data = await request.get_json()
    # test = data.decode('utf-8') 
    # print(test,type(test))
    r = redis.Redis(host='localhost',port=6379,db=0)
    username = data["username"]
    score = data["score"]
    print(data,data["username"])
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

    #get list of users and their scores
    scoreL = r.zrange("username",0,-1,withscores=True)
    result = {}
    print(len(scoreL), scoreL)
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


# def log_request(request):
#     print(f"Request event hook: {request.method} {request.url} - Waiting for response")

# def log_response(response):
#     request = response.request
#     print(f"Response event hook: {request.method} {request.url} - Status {response.status_code}")
