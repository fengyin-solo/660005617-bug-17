import { defineStore } from 'pinia'
import { ref } from 'vue'
import axios from 'axios'
import type { DAGWorkflow, ExecutionInfo } from '@/types'
export const useDAGStore = defineStore('dag', () => {
  const loading = ref(false)
  const workflow = ref<DAGWorkflow | null>(null)
  const execution = ref<ExecutionInfo | null>(null)
  const wsConnected = ref(false)
  const workers = ref(3)
  const strategy = ref('fifo')

  let ws: WebSocket | null = null

  // 只接受“当前这次执行”的推送；旧运行（或上个页面会话）迟到的消息直接丢弃，
  // 从根上保证同一 runId 口径下两次查看结果一致、旧分不残留。
  function applyExecution(d: ExecutionInfo) {
    const currentRunId = execution.value?.runId ?? null
    if (currentRunId != null && d.runId != null && d.runId < currentRunId) return
    execution.value = d
  }

  function connectWS() {
    ws = new WebSocket(`ws://${location.hostname}:8000/ws`)
    ws.onopen = () => { wsConnected.value = true }
    ws.onmessage = (e) => {
      try { applyExecution(JSON.parse(e.data) as ExecutionInfo) } catch { /* 忽略无法解析的帧 */ }
    }
    ws.onclose = () => { wsConnected.value = false }
  }

  async function createWorkflow(name: string) {
    loading.value = true
    try { const { data } = await axios.post('/api/workflow', { name }); workflow.value = data }
    finally { loading.value = false }
  }

  async function run() {
    if (!workflow.value) return
    loading.value = true
    try {
      const { data } = await axios.post('/api/run', {
        workflowId: workflow.value.id, workers: workers.value, strategy: strategy.value
      })
      // 整体替换为新执行的快照：旧执行的环节分数不会带入新一轮计算
      execution.value = data as ExecutionInfo
    } finally { loading.value = false }
  }

  // 页面重开时向服务端要最近一次执行的冻结结果，而不是沿用任何本地残留
  async function hydrateLatest() {
    try {
      const { data } = await axios.get<ExecutionInfo & { workflow: DAGWorkflow | null }>('/api/runs/latest')
      if (data && data.runId != null) execution.value = data as ExecutionInfo
    } catch { /* 尚无执行记录时保持空态 */ }
  }

  function disconnectWS() { ws?.close(); ws = null }
  return { loading, workflow, execution, wsConnected, workers, strategy, connectWS, createWorkflow, run, hydrateLatest, disconnectWS }
})
