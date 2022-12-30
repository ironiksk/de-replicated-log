import os
import argparse
import logging
import asyncio
import uvicorn
from fastapi import FastAPI, Response, Request
from fastapi.responses import JSONResponse
import time
from threading import Thread, Condition

# from omegaconf import OmegaConf, MISSING
# from base_cli import BaseCLI
from const import LogRequest, LogListResponse, Item, LogNodeType, req2item, item2resp, RegisterSecondaryRequest
from rlog import RLogServer


logging.basicConfig(level=logging.INFO)
logging.info('Initializing service')


parser = argparse.ArgumentParser()
parser.add_argument('--port', type=int, default=int(os.environ.get("PORT", 8080)), help='Service port')
# parser.add_argument('-n', '--nodes', nargs='+', default=[], help='List of URLs to nodes (secondaries)')
parser.add_argument('-m', '--master_url', type=str, default=None, help='Do not set if node is master')
parser.add_argument('-u', '--url', type=str, help='URL to access this service')
args = parser.parse_args()


role = 'secondary' if args.master_url else 'master'
RLOG = RLogServer(url=args.url, role=role)
# if role == 'secondary':
#     RLOG.add_remote_node(args.master_url)


app = FastAPI(
    title="Replicated Log",
    version="1.0",
    debug=True)


@app.on_event("startup")
async def startup_event():
    def _register_on_master_task():
        time.sleep(1)
        RLOG.add_remote_node(args.master_url)    
    # UGLY but secondaary should start before it sends req to master and register
    if role == 'secondary':
        t = Thread(target=_register_on_master_task)
        t.start()
    
        


@app.post("/log/{log_id}", name="log:append_known")
def log_append_id(log_id: str, req: LogRequest):
    item = req2item(req)
    item.id = log_id
    _item = RLOG.append(item)
    return item2resp(_item)


@app.get("/log/{log_id}", name="log:get_known")
def log_get_id(log_id: str):
    _item = RLOG.get(log_id)
    return item2resp(_item)


@app.post("/log", response_model=LogRequest, name="log:append_new")
def log_append(req: LogRequest):
    l = req2item(req)
    _item = RLOG.append(l)
    return item2resp(_item)


@app.get("/logs", name="logs:get")
def logs_get(r:int=1):
    items = RLOG.get_all(r=int(r))
    return [item2resp(it) for it in items]


@app.post("/register", name="node:register")
async def register_secondary(req: RegisterSecondaryRequest): # , request: Request):
    RLOG.add_remote_node(req.url)
    return JSONResponse({'status': 'success'})

@app.get("/healthcheck")
def healthcheck():
    return JSONResponse({'status': 'success'})

@app.get("/info")
def info():
    return JSONResponse({ 
        'node_id': RLOG.node.id, 
        'role': RLOG.node.role, 
        'version': RLOG.node.data_version})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=args.port)
    RLOG.stop()


