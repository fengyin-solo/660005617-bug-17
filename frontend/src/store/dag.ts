import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'
import type { DAGWorkflow, ExecutionInfo, RunScore } from '@/types'
export const useDAGStore = defineStore('dag', () => {
  const loading = ref(false)
  const workflow = ref<DAGWorkflow | null>(null)
  const execution = ref<ExecutionInfo | null>(null)
  const wsConnected = ref(false)
  const workers = ref(3)
  const strategy = ref('fifo')

  // 全应用唯一的评分来源：只取服务端冻结在执行快照里的 score
  const score = computed<RunScore | null>(() =>
    execution.value?.completed ? execution.value.score : null
  )

  let ws: WebSocket|null = null
  function applyExecution(d: ExecutionInfo | null) {
    if (!d) return
    // 只接受当前或更新的执行，旧执行的迟到推送不能覆盖新结果
    if (execution.value && d.runId < execution.value.runId) return
    execution.value = d
  }

  function connectWS() {
    ws = new WebSocket(`ws://${location.hostname}:8000/ws`)
    ws.onopen = () => { wsConnected.value = true }
    ws.onmessage = (e) => {
      try { applyExecution(JSON.parse(e.data)) }
      catch {}
    }
    ws.onclose = () => { wsConnected.value = false }
  }

  async function createWorkflow(name: string) {
    loading.value = true
    try { const { data } = await axios.post('/api/workflow', { name }) ; workflow.value = data }
    finally { loading.value = false }
  }

  async function run() {
    if (!workflow.value) return
    loading.value = true
    try {
      const { data } = await axios.post('/api/run', {
        workflowId: workflow.value.id, workers: workers.value, strategy: strategy.value
      })
      // 新执行整体替换旧执行，旧分数不会残留
      execution.value = data
    } finally { loading.value = false }
  }

  // 重开页面时拉取服务端最近一次冻结结果
  async function fetchLatest() {
    try {
      const { data } = await axios.get<ExecutionInfo | null>('/api/runs/latest')
      applyExecution(data)
    } catch {}
  }

  function disconnectWS() { ws?.close(); ws = null }
  return { loading, workflow, execution, score, wsConnected, workers, strategy,
    connectWS, createWorkflow, run, fetchLatest, disconnectWS }
})
