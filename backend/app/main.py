import asyncio, time, random, json, threading
from collections import defaultdict, deque
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ACTIVE_CLIENTS = []
WORKFLOW_ID = 0
RUN_ID = 0
RUNS = {}          # runId -> 冻结/进行中的执行结果快照
RUNS_LOCK = threading.Lock()
LOOP = None        # 主事件循环，工作线程通过它推送 WebSocket 消息


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


@app.on_event("startup")
def _capture_loop():
    # execute_workflow 运行在独立线程中，推送 WebSocket 必须使用主事件循环
    global LOOP
    LOOP = asyncio.get_event_loop()


@app.post("/api/workflow")
def create_workflow(req: WorkflowCreate):
    global WORKFLOW_ID
    WORKFLOW_ID += 1
    dag = generate_dag_workflow(req.name)
    return {"id": WORKFLOW_ID, "name": req.name, "nodes": dag["nodes"], "edges": dag["edges"],
            "_durations": dag["durations"]}


def build_scores(nodes, durations):
    """按同一次执行的最终口径计算一次综合评分，结果随执行冻结，不再重算。

    每个环节独立打分，总分 = 全部环节得分的算术平均（保留整数）。
    """
    stages = []
    for n in nodes:
        reasons = []
        if n["status"] != "SUCCESS":
            # FAILED / TIMEOUT / 因上游失败而 PENDING（未执行）一律 0 分
            score = 0
            reasons.append("执行未成功")
        else:
            score = 100
            # 重试扣分：每次重试 -10
            if n["retries"] > 0:
                penalty = min(n["retries"] * 10, 30)
                score -= penalty
                reasons.append(f"重试{n['retries']}次(-{penalty})")
            # 耗时超标扣分：实际耗时超出基准 20% 后，每超 10% 扣 5 分，封顶 30
            if n["startTime"] and n["endTime"]:
                base = durations.get(n["id"], 0)
                if base > 0:
                    ratio = (n["endTime"] - n["startTime"]) / base
                    if ratio > 1.2:
                        penalty = min(int((ratio - 1.2) * 10) * 5, 30)
                        if penalty > 0:
                            score -= penalty
                            reasons.append(f"耗时超标{int((ratio - 1) * 100)}%(-{penalty})")
            score = max(0, score)
        stages.append({
            "taskId": n["id"],
            "name": n["name"],
            "status": n["status"],
            "score": score,
            "reasons": reasons,
        })
    total = round(sum(s["score"] for s in stages) / len(stages)) if stages else 0
    return {"totalScore": total, "stages": stages}


@app.post("/api/run")
def run_workflow(req: RunRequest):
    global RUN_ID, LATEST_RUN_ID
    RUN_ID += 1
    run_id = RUN_ID
    dag = generate_dag_workflow("workflow")

    snapshot = {
        "runId": run_id,
        "workflow": {"id": req.workflowId, "name": "workflow", "nodes": dag["nodes"], "edges": dag["edges"]},
        "logs": [],
        "circuitBreakers": [],
        "completed": False,
        "scores": None,       # 执行中不出分，避免概览出现“越用越乱”的中间分数
        "startedAt": time.time(),
        "completedAt": None,
    }
    with RUNS_LOCK:
        RUNS[run_id] = snapshot
        LATEST_RUN_ID = run_id

    t = threading.Thread(target=execute_workflow,
                         args=(run_id, dag, req.workflowId, req.workers, req.strategy), daemon=True)
    t.start()
    return snapshot


@app.get("/api/runs/latest")
def latest_run():
    """重开页面时取回最近一次执行的冻结结果，旧分数不依赖浏览器本地残留。"""
    with RUNS_LOCK:
        if not RUNS:
            return {"runId": None, "scores": None, "completed": False, "workflow": None,
                    "logs": [], "circuitBreakers": [], "startedAt": None, "completedAt": None}
        return RUNS[LATEST_RUN_ID]


def execute_workflow(run_id, dag, workflow_id, workers, strategy):
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
    cb_state = defaultdict(lambda: {"failureCount": 0, "state": "CLOSED", "cooldownUntil": 0, "everOpened": False})
    failure_threshold = 3
    running_tasks = {}
    completed = set()

    def send_update(completed_flag=False, scores=None):
        cb_list = [{"taskId": k, **v} for k, v in cb_state.items()]
        payload = {
            "runId": run_id,
            "workflow": {"id": workflow_id, "name": "workflow", "nodes": nodes, "edges": edges},
            "logs": logs[-30:],
            "circuitBreakers": cb_list,
            "completed": completed_flag,
            # 执行过程中为 null；完成时携带按本次执行一次性算出并冻结的评分
            "scores": scores,
            "startedAt": RUNS[run_id]["startedAt"],
            "completedAt": time.time() if completed_flag else None,
        }
        if completed_flag or not RUNS[run_id]["completed"]:
            with RUNS_LOCK:
                RUNS[run_id] = payload
        for ws in ACTIVE_CLIENTS:
            asyncio.run_coroutine_threadsafe(ws.send_text(json.dumps(payload)), LOOP)
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
                        cb["everOpened"] = True
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

    # 评分只在执行完成时按本次执行的最终数据计算一次，之后各处读取同一冻结快照
    scores = build_scores(nodes, durations)
    send_update(True, scores)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ACTIVE_CLIENTS.append(ws)
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if ws in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(ws)
