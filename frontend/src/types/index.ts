export interface TaskNode { id: string; name: string; deps: string[]; x: number; y: number; status: string; startTime?: number; endTime?: number; retries: number }
export interface DAGWorkflow { id: number; name: string; nodes: TaskNode[]; edges: [string,string][] }
export interface ExecutionLog { taskId: string; status: string; timestamp: number; message: string }
export interface CircuitBreaker { taskId: string; failureCount: number; state: string; cooldownUntil: number; everOpened?: boolean }
export interface StageScore { taskId: string; name: string; status: string; score: number; reasons: string[] }
export interface ExecutionScores { totalScore: number; stages: StageScore[] }
export interface ExecutionInfo {
  runId: number | null
  workflow: DAGWorkflow
  logs: ExecutionLog[]
  circuitBreakers: CircuitBreaker[]
  completed: boolean
  scores: ExecutionScores | null
  startedAt: number | null
  completedAt: number | null
}
