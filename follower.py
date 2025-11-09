#FOLLOWER

from fastapi import FastAPI, HTTPException, Request, Response
import asyncio
import redis
import os
import requests

isleader=False

is_catching_up=True

MY_ID="node2" #follower start state

RENEW_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('setex', KEYS[1], ARGV[2], ARGV[1])
else
  return 0
end
"""

OTHER_BROKER_IP="IP" # zerotier ip

TIMELOCK_TIME=5 #5s for time locked leader lease expiry
RENEW_LEASE=2 #how often leader renews
FOLLOWER_CHECK_INTERVAL=1 #how often follower checks

TOPIC_1="topic1"
TOPIC_2="topic2"
TOPIC_3="topic3"

file_name="log1.txt"

topics={
        TOPIC_1:[os.path.join(TOPIC_1, file_name), 0], #log file path per topic, offset for that file
        TOPIC_2:[os.path.join(TOPIC_2, file_name),0],
        TOPIC_3:[os.path.join(TOPIC_3, file_name),0],
        }


#connecting to redis to be able to access HWM and leader lease
r=redis.Redis(
    host='REDIS_URL',
    port=REDIS_PORT,
    decode_responses=True,
    username="username",
    password="*",
)

app=FastAPI()

def set_hwm(topic: str, hwm: int):
    print("setting hwm")
    r.hset(f"hwm:{topic}", "offset", hwm)  #key, field="offset", value

def get_hwm(topic: str) -> int:
    print("getting hwm")
    val=r.hget(f"hwm:{topic}", "offset")
    return int(val) if val is not None else 0

@app.post("/produce/")
async def produce(request: Request):
    print("produce accessed!!")
    if not isleader:
        raise HTTPException(403, "Follower cannot accept produce")
    
    topic=request.headers.get("Topic")
    if not topic:
        raise HTTPException(400,"Missing topic in headers!!\n")

    data: bytes=await request.body()
    
    
    #replicate code:
    replicate_url = f"http://{OTHER_BROKER_IP}:8000/internal/replicate"
    topic_header={"Topic":topic}
    try:
        response=requests.post(replicate_url, headers=topic_header, data=data,timeout=2)
    
        if response.status_code==200:
            print("Follower ACK:", response.json())
        else:
            print("Follower not reachable, continuing...")
    except Exception as e:
        print(f"Follower unreachable, degraded mode... error:{e}")
    with open(topics[topic][0],'ab') as f:
        f.write(data+b"\n")
    topics[topic][1]+=(len(data)+1)
    print(f"Written {len(data)+1} bytes to {topic} dir's log file")
    set_hwm(topic,topics[topic][1])
    return topics[topic][1]
    
@app.get("/consume/")
async def handle_consume(topic :str, offset :int): #assume consumer sends GET /consume/topic?=topic1&&offset?=5
    print("handle_consumer")
    if not isleader:
        raise HTTPException(403, "Follower cannot accept consume")
    
    try:
        hwm=get_hwm(topic)
        if(offset<0 or offset>hwm): #if offset neg or greater than HWM
            raise HTTPException(400,"Invalid offset value\n")
        
        with open(topics[topic][0],"rb") as f:
            f.seek(offset)
            msg=f.read(hwm-offset)
        return Response(content=msg, media_type="application/octet-stream")
    
    except FileNotFoundError:
        print("No such topic found :(")

@app.post("/internal/replicate")
async def create_replica(request: Request):
    if isleader:
        raise HTTPException(403, "Leader cannot accept replicate")

    if is_catching_up:
        return {"status": "skipped during catchup"}
    
    topic=request.headers.get("Topic")
    if not topic:
        raise HTTPException(400,"Missing topic in headers!!\n")
    
    msg_id=request.headers.get("Idempotency-Key")
    if msg_id:
        # If already replicated, skip write
        if r.sismember("replicated_ids", msg_id):
            return {"status": "duplicate skipped"}

    
    data: bytes=await request.body()

    with open(topics[topic][0],'ab') as f:
        f.write(data+b"\n")

    topics[topic][1]+=(len(data)+1)
    print(f"Written {len(data)+1} bytes to {topic} dir's log file")

    if msg_id:
        r.sadd("replicated_ids", msg_id)

    return {"status_code":200}

@app.get("/metadata/leader")
async def metadata_leader():
    print("metadata/leader")
    return {"leader":r.get("leader")}

async def lease_manager():
    print("lease_manager")
    global isleader
    key="leader"
    while True:
        try:
            if isleader:
                try:
                    res=r.eval(RENEW_LUA,1,key,MY_ID,TIMELOCK_TIME)
                    if res==0:
                        print("Lost leadership (renew failed)")
                        isleader=False
                    else:
                        pass #renewed successfully
                except Exception as e:
                    print("Error renewing leadership lease:", e)
            else:
                acquired=False
                try:
                    acquired = r.set(key, MY_ID, ex=TIMELOCK_TIME, nx=True)
                except Exception as e:
                    print("Error acquiring leadership lease:", e)
                if acquired:
                    print(f"{MY_ID} acquired leadership")
                    for topic_name, topic_data in topics.items():
                        if os.path.exists(topic_data[0]):
                            topics[topic_name][1]=os.path.getsize(topic_data[0])
                    isleader=True
                else:
                    pass

        except Exception as e:
            print("lease_manager error:", e)
            isleader=False

        await asyncio.sleep(RENEW_LEASE if isleader else FOLLOWER_CHECK_INTERVAL)

async def catch_up_if_behind():

    #When follower starts up and realizes it's behind HWM, fetches missing data from the current leader

    await asyncio.sleep(2)  #Waiting for lease_manager to stabilize
    
    if isleader:
        return
    
    print("Checking if catch-up is needed...")
    
    for topic in topics.keys():
        local_offset=topics[topic][1] #alr read on startup when follower starts up
        redis_hwm=get_hwm(topic)
        
        if local_offset<redis_hwm:
            print(f"NODE IS BEHIND on {topic}: local offset={local_offset}, HWM={redis_hwm}")
            print(f"Fetching missing data from current leader...")
            
            try:
                #Fetch missing data from current leader
                response=requests.get(
                    f"http://{OTHER_BROKER_IP}:8000/consume/",
                    params={"topic": topic, "offset": local_offset},
                    timeout=5
                )
                
                if response.status_code==200:
                    missing_data=response.content
                    
                    #Write missing data to local log file
                    with open(topics[topic][0], 'ab') as f:
                        f.write(missing_data)
                    
                    #Update local offset to match HWM
                    topics[topic][1]=redis_hwm
                    print(f"Caught up on {topic}! Now at offset {redis_hwm}!")
                else:
                    print(f"Catch-up failed: {response.status_code}")
                    
            except Exception as e:
                print(f"Catch-up error for {topic}: {e}")
        else:
            print(f"{topic} is up-to-date! (offset={local_offset})")
    global is_catching_up
    is_catching_up=False

@app.on_event("startup")
async def start_tasks():
    for key in topics.keys():
        os.makedirs(key, exist_ok=True)
        log_path=os.path.join(key, file_name)
        
        #Load both curr on disk offset and Redis HWM, use the minimum
        disk_offset=os.path.getsize(log_path) if os.path.exists(log_path) else 0
        redis_hwm=get_hwm(key)
        actual_offset=min(redis_hwm, disk_offset) if redis_hwm > 0 else disk_offset
        
        topics[key]=[log_path, actual_offset]

    asyncio.create_task(lease_manager())
    asyncio.create_task(catch_up_if_behind())
    print("STARTED!!!!")