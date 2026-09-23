import asyncio, time, random, json, threading, copy
from collections import defaultdict, deque
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ACTIVE_CLIENTS = []
WORKFLOW_ID = 0

# 已完成执行的冻结快照：评分只在这里算一次，HTTP / WebSocket / 重开页面都读同一份
STATE_LOCK = threading.Lock()
RUN_SEQ = 0
LATEST_RUN = None
EVENT_LOOP = None


@app.on_event("startup")
def _capture_event_loop():
    # 执行线程里 asyncio.get_event_loop() 拿不到循环，启动时先抓住
    global EVENT_LOOP
    EVENT_LOOP = asyncio.get_event_loop()

class WorkflowCreate(BaseModel):
    name: str = "data-pipeline"

class RunRequest(BaseModel):
    workflowId: int
    workers: int = 3
    strategy: str = "fifo"


def generate_dag_workflow(name: str):
    """Create a realistic DAG pipeline"""
    nodes = [
        {"id": "extract", "name": "数据提取", "deps": [], "duration": 2.0},
        {"id": "validate", "name": "数据校验", "deps": ["extract"], "duration": 1.5},
        {"id": "clean_a", "name": "清洗分支A", "deps": ["validate"], "duration": 1.8},
        {"id": "clean_b", "name": "清洗分支B", "deps": ["validate"], "duration": 1.2},
        {"id": "transform", "name": "数据转换", "deps": ["clean_a"], "duration": 3.0},
        {"id": "enrich", "name": "数据增强", "deps": ["clean_a", "clean_b"], "duration": 2.0},
        {"id": "aggregate", "name": "聚合计算", "deps": ["transform", "enrich"], "duration": 2.5},
        {"id": "quality", "name": "质量检查", "deps": ["aggregate"], "duration": 1.0},
        {"id": "export_db", "name": "入库", "deps": ["quality"], "duration": 1.8},
        {"id": "export_report", "name": "报表生成", "deps": ["quality"], "duration": 2.2},
        {"id": "notify", "name": "通知", "deps": ["export_db", "export_report"], "duration": 0.5},
    ]
    positions = [
        (0, 0), (0, 1), (-1, 2), (1, 2), (-1, 3),
        (0.5, 3), (-0.3, 4), (-0.3, 5), (-1, 6), (0.5, 6), (-0.3, 7)
    ]
    for i, n in enumerate(nodes):
        n["x"] = positions[i][0] * 2.5 + 2.5
        n["y"] = positions[i][1] * 0.9
        n["status"] = "PENDING"
        n["retries"] = 0
        n["startTime"] = None
        n["endTime"] = None

    edges = []
    for n in nodes:
        for d in n["deps"]:
            edges.append([d, n["id"]])

    return {"nodes": [{
        "id": n["id"], "name": n["name"], "deps": n["deps"],
        "x": n["x"], "y": n["y"], "status": n["status"],
        "startTime": None, "endTime": None, "retries": n["retries"]
    } for n in nodes], "edges": edges, "durations": {n["id"]: n["duration"] for n in nodes}}


def compute_node_score(node, expected_duration):
    """按同一次执行的最终状态给单个环节打分（确定性，不引入新的随机量）"""
    if node["status"] == "SUCCESS":
        base = 100
    elif node["status"] in ("FAILED", "TIMEOUT"):
        base = 40
    else:
        base = 0  # PENDING/RUNNING 不应出现在冻结快照里，兜底给 0

    score = base - node["retries"] * 15  # 每次重试扣 15 分

    # 按本次实际耗时与预期耗时的偏差微调（±10 分）
    if node["startTime"] and node["endTime"] and expected_duration:
        ratio = (node["endTime"] - node["startTime"]) / expected_duration
        score += round(max(-1.0, min(1.0, 1.0 - ratio)) * 10)

    return max(0, min(100, score))


def build_score(run_id, nodes, durations):
    """执行结束时对同一次执行的最终状态算一次分，算完即冻结"""
    items = [{
        "taskId": n["id"],
        "name": n["name"],
        "status": n["status"],
        "retries": n["retries"],
        "score": compute_node_score(n, durations.get(n["id"]))
    } for n in nodes]
    total = round(sum(i["score"] for i in items) / len(items)) if items else 0
    return {"runId": run_id, "total": total, "items": items, "frozen": True}


def build_snapshot(run_id, nodes, edges, logs, cb_state, completed, score=None):
    """组装同一份执行快照；所有出口（HTTP / WebSocket / 重开查询）都走这里"""
    return {
        "runId": run_id,
        "workflow": {"id": 1, "name": "workflow",
                     "nodes": copy.deepcopy(nodes), "edges": edges},
        "logs": logs[-30:],
        "circuitBreakers": [{"taskId": k, **v} for k, v in cb_state.items()],
        "completed": completed,
        "score": score,
    }


def broadcast(payload):
    if not ACTIVE_CLIENTS or EVENT_LOOP is None:
        return
    data = json.dumps(payload)
    dead = []
    for ws in list(ACTIVE_CLIENTS):
        try:
            asyncio.run_coroutine_threadsafe(ws.send_text(data), EVENT_LOOP)
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(ws)


@app.post("/api/workflow")
def create_workflow(req: WorkflowCreate):
    global WORKFLOW_ID
    WORKFLOW_ID += 1
    dag = generate_dag_workflow(req.name)
    return {"id": WORKFLOW_ID, "name": req.name, "nodes": dag["nodes"], "edges": dag["edges"],
            "_durations": dag["durations"]}


@app.post("/api/run")
def run_workflow(req: RunRequest):
    dag = generate_dag_workflow("workflow")
    global RUN_SEQ
    with STATE_LOCK:
        RUN_SEQ += 1
        run_id = RUN_SEQ
    t = threading.Thread(target=execute_workflow,
                         args=(dag, req.workers, req.strategy, run_id), daemon=True)
    t.start()
    return {
        "runId": run_id,
        "workflow": {"id": req.workflowId, "name": "workflow", "nodes": dag["nodes"], "edges": dag["edges"]},
        "logs": [], "circuitBreakers": [], "completed": False, "score": None
    }


def execute_workflow(dag, workers, strategy, run_id):
    nodes = dag["nodes"]
    durations = dag["durations"]
    edges = dag["edges"]
    in_degree = defaultdict(int)
    adj = defaultdict(list)
    for u, v in edges:
        in_degree[v] += 1
        adj[u].append(v)

    # BFS topological sort
    ready = deque([n["id"] for n in nodes if in_degree[n["id"]] == 0])
    node_map = {n["id"]: n for n in nodes}
    logs = []
    cb_state = defaultdict(lambda: {"failureCount": 0, "state": "CLOSED", "cooldownUntil": 0})
    failure_threshold = 3
    running_tasks = {}
    completed = set()

    def send_update(completed_flag=False, score=None):
        broadcast(build_snapshot(run_id, nodes, edges, logs, cb_state,
                                 completed_flag, score=score))
        time.sleep(0.3)

    while ready or running_tasks:
        # Start tasks
        while ready and len(running_tasks) < workers:
            tid = ready.popleft()
            node = node_map[tid]
            cb = cb_state[tid]
            if cb["state"] == "OPEN" and time.time() < cb["cooldownUntil"]:
                ready.appendleft(tid)
                continue
            if cb["state"] == "OPEN":
                cb["state"] = "HALF_OPEN"

            node["status"] = "RUNNING"
            node["startTime"] = time.time()

            # Simulate task execution (random success/failure)
            will_fail = random.random() < 0.12  # 12% failure rate
            runtime = durations.get(tid, 1.5) * random.uniform(0.7, 1.3)
            running_tasks[tid] = {
                "end_time": time.time() + runtime,
                "will_fail": will_fail,
                "retries": node["retries"]
            }
            logs.append({"taskId": tid, "status": "RUNNING", "timestamp": time.time(), "message": f"开始执行 {node['name']}"})

        # Check completed tasks
        now = time.time()
        finished = []
        for tid, info in running_tasks.items():
            if now >= info["end_time"]:
                node = node_map[tid]
                if info["will_fail"] and node["retries"] < 3:
                    node["retries"] += 1
                    node["status"] = "PENDING"
                    ready.appendleft(tid)
                    cb = cb_state[tid]
                    cb["failureCount"] += 1
                    logs.append({"taskId": tid, "status": "FAILED", "timestamp": now, "message": f"重试 {node['retries']}/3"})
                    if cb["failureCount"] >= failure_threshold:
                        cb["state"] = "OPEN"
                        cb["cooldownUntil"] = now + 5
                        logs.append({"taskId": tid, "status": "CIRCUIT_OPEN", "timestamp": now, "message": f"熔断! {failure_threshold}次连续失败"})
                else:
                    node["status"] = "SUCCESS"
                    node["endTime"] = now
                    completed.add(tid)
                    cb_state[tid]["failureCount"] = 0
                    cb_state[tid]["state"] = "CLOSED"
                    logs.append({"taskId": tid, "status": "SUCCESS", "timestamp": now, "message": f"完成 {node['name']}"})
                    for next_tid in adj[tid]:
                        in_degree[next_tid] -= 1
                        if in_degree[next_tid] == 0:
                            ready.append(next_tid)
                finished.append(tid)

        for tid in finished:
            del running_tasks[tid]

        send_update()
        if len(completed) == len(nodes):
            break

    # 同一次执行到此为止：按最终状态算一次分，冻结成快照，之后不再变动
    score = build_score(run_id, nodes, durations)
    snapshot = build_snapshot(run_id, nodes, edges, logs, cb_state, True, score=score)
    global LATEST_RUN
    with STATE_LOCK:
        LATEST_RUN = snapshot
    broadcast(snapshot)


@app.get("/api/runs/latest")
def get_latest_run():
    """重开页面时取最近一次已经冻结的执行结果（含评分）"""
    with STATE_LOCK:
        return copy.deepcopy(LATEST_RUN) if LATEST_RUN else None


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ACTIVE_CLIENTS.append(ws)
    # 新连接立刻收到最近一次冻结结果，重开页面不会停留在旧的本地残留上
    with STATE_LOCK:
        snapshot = copy.deepcopy(LATEST_RUN) if LATEST_RUN else None
    if snapshot is not None:
        try:
            await ws.send_text(json.dumps(snapshot))
        except Exception:
            if ws in ACTIVE_CLIENTS:
                ACTIVE_CLIENTS.remove(ws)
            return
    try:
        while True: await ws.receive_text()
    except:
        if ws in ACTIVE_CLIENTS: ACTIVE_CLIENTS.remove(ws)