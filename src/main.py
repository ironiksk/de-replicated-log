import os
import argparse
import logging

import uvicorn
from fastapi import FastAPI, Response, Request
from fastapi.responses import JSONResponse

# from omegaconf import OmegaConf, MISSING
# from base_cli import BaseCLI
from const import LogRequest, LogResponse, LogListResponse, Item, LogNodeType, req2item, item2resp, RegisterSecondaryRequest
from rlog import rlog_builder


logging.basicConfig(level=logging.DEBUG)
logging.info('Initializing service')


parser = argparse.ArgumentParser()
parser.add_argument('-r', '--role', type=LogNodeType, choices=list(LogNodeType), default=LogNodeType.MASTER)
parser.add_argument('-m', '--master', type=str, default=None, help='URL to master')
parser.add_argument('--port', type=int, default=int(os.environ.get("PORT", 8080)), help='Service port')
args = parser.parse_args()


RLOG = rlog_builder(role=args.role, master=args.master, port=args.port)


app = FastAPI(
    title="Replicated Log",
    version="1.0",
    debug=True)


@app.post("/log/{log_id}", name="log:append_known")
async def log_append_id(log_id: str, req: LogRequest):
    item = req2item(req)
    item.id = log_id
    _item = await RLOG.append(item)
    return item2resp(_item)


@app.post("/log", response_model=LogResponse, name="log:append_new")
async def log_append(req: LogRequest):
    _item = await RLOG.append(req2item(req))
    return item2resp(_item)


@app.get("/logs", response_model=LogListResponse, name="logs:get")
async def logs_get():
    items = await RLOG.get_all()
    return LogListResponse(logs=[item2resp(it) for it in items])


@app.post("/register", name="master:register")
async def register_secondary(secondary: RegisterSecondaryRequest, request: Request):
    await RLOG.add_secondary(secondary.node_id, f'http://{request.client.host}:{secondary.port}')
    # resp = RegisterSecondaryResponse(status=True)
    # return resp
    return JSONResponse({'status': 'success'})


@app.get("/healthcheck")
def healthcheck():
    return JSONResponse({'status': 'success'})


if __name__ == "__main__":
    uvicorn.run(app, debug=False, host="0.0.0.0", port=args.port)


