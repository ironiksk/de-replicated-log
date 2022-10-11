import os
import argparse
import logging

import uvicorn
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse

# from omegaconf import OmegaConf, MISSING
# from base_cli import BaseCLI
from const import Message, MessageResponse, MessageListResponse
from rlog import RLog, RLogMaster


logging.basicConfig(level=logging.DEBUG)
logging.info('Initializing service')


parser = argparse.ArgumentParser()
parser.add_argument('-r', '--role', type=str, default='MASTER')
parser.add_argument('-s', '--secondaries', nargs='+', default=[])
parser.add_argument('-m', '--master', type=str, default=None)

args = parser.parse_args()
ROLE = args.role


# RLOG = RLog()
RLOG = RLogMaster(role=args.role, master=args.master, replicas=args.secondaries)


app = FastAPI(
    title="Replicated Log",
    version="1.0",
    debug=True)


@app.post("/log", response_model=MessageResponse, name="log:append")
async def log_append(req: Message):
    logging.debug(req)
    _id, _msg = await RLOG.append(req.msg)
    return MessageResponse(id=_id, msg=_msg)


@app.get("/logs", response_model=MessageListResponse, name="logs:get")
async def logs_get():
    return MessageListResponse(msgs=[
        MessageResponse(id=_id, msg=_msg) for (_id, _msg) in RLOG.get_all()
    ])


@app.get("/healthcheck")
def healthcheck():
    return JSONResponse({'status': 'success'})


if __name__ == "__main__":
    uvicorn.run(app, debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))


