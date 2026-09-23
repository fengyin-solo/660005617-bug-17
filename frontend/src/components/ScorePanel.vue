<template>
  <div class="panel">
    <h4>🏆 综合评分</h4>

    <div v-if="!snapshot" class="empty">暂无执行记录</div>

    <div v-else-if="!snapshot.completed || !snapshot.scores" class="scoring">
      <div class="overview running">
        <div class="ov-num">--</div>
        <div class="ov-meta">
          <div class="ov-label">执行 #{{ snapshot.runId }} 评分计算中</div>
          <div class="ov-sub">完成后按本次执行一次性算定</div>
        </div>
      </div>
    </div>

    <template v-else>
      <!-- 概览：直接读取本次执行冻结快照里的总分 -->
      <div class="overview">
        <div class="ov-num" :class="scoreLevel(snapshot.scores.totalScore)">
          {{ snapshot.scores.totalScore }}
        </div>
        <div class="ov-meta">
          <div class="ov-label">执行 #{{ snapshot.runId }} 总分</div>
          <div class="ov-sub">{{ formatTime(snapshot.completedAt) }} 冻结</div>
        </div>
      </div>

      <!-- 评分列表：与总分同属一个 scores 快照，不做任何本地累加/缓存 -->
      <div class="stage-list">
        <div v-for="s in snapshot.scores.stages" :key="s.taskId" class="stage-row" :class="s.status.toLowerCase()">
          <div class="st-head">
            <span class="st-name">{{ s.name }}</span>
            <span class="st-score" :class="scoreLevel(s.score)">{{ s.score }}</span>
          </div>
          <div class="st-reasons">
            <span v-if="!s.reasons.length" class="st-ok">正常</span>
            <span v-for="(r, i) in s.reasons" :key="i" class="st-tag">{{ r }}</span>
          </div>
        </div>
      </div>

      <div class="recheck">
        明细均值 {{ listAverage }} ｜ 总分 {{ snapshot.scores.totalScore }}
        <span :class="consistent ? 'match' : 'mismatch'">
          {{ consistent ? '✓ 口径一致' : '✗ 总分与明细不符' }}
        </span>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useDAGStore } from '../store/dag'
import type { ExecutionInfo } from '../types'

const store = useDAGStore()
const snapshot = computed<ExecutionInfo | null>(() => store.execution)

// 按列表中实际展示的环节分数复算均值，验证与冻结总分一致
const listAverage = computed(() => {
  const stages = snapshot.value?.scores?.stages ?? []
  if (!stages.length) return 0
  return Math.round(stages.reduce((sum, s) => sum + s.score, 0) / stages.length)
})
const consistent = computed(() =>
  snapshot.value?.scores != null && listAverage.value === snapshot.value.scores.totalScore
)

function scoreLevel(score: number) {
  if (score >= 85) return 'good'
  if (score >= 60) return 'mid'
  return 'bad'
}
function formatTime(ts: number | null) {
  if (!ts) return ''
  return new Date(ts * 1000).toLocaleTimeString('zh-CN', { hour12: false })
}
</script>

<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a}
.panel h4{color:#bb86fc;font-size:12px;margin-bottom:8px}
.empty{color:#4a5568;font-size:11px}
.overview{display:flex;align-items:center;gap:12px;margin-bottom:8px}
.ov-num{font-size:34px;font-weight:800;line-height:1;color:#e0e0e0;min-width:64px;text-align:center}
.ov-num.good{color:#22c55e}.ov-num.mid{color:#fbbf24}.ov-num.bad{color:#ef4444}
.overview.running .ov-num{color:#4a5568;font-size:22px}
.ov-label{font-size:12px;color:#ccc;font-weight:600}
.ov-sub{font-size:10px;color:#666;margin-top:2px}
.stage-list{max-height:220px;overflow-y:auto}
.stage-row{padding:4px 6px;border-radius:4px;margin:2px 0;background:#14142b}
.stage-row:not(.success){border-left:2px solid #ef4444}
.st-head{display:flex;justify-content:space-between;align-items:center}
.st-name{font-size:11px;color:#ddd}
.st-score{font-size:13px;font-weight:700;color:#e0e0e0}
.st-score.good{color:#22c55e}.st-score.mid{color:#fbbf24}.st-score.bad{color:#ef4444}
.st-reasons{display:flex;flex-wrap:wrap;gap:4px;margin-top:2px}
.st-ok{font-size:9px;color:#22c55e}
.st-tag{font-size:9px;color:#fbbf24;background:#fbbf2415;border-radius:2px;padding:0 4px}
.recheck{margin-top:6px;font-size:10px;color:#888;border-top:1px solid #2a2a4a;padding-top:6px}
.recheck .match{color:#22c55e;margin-left:4px}
.recheck .mismatch{color:#ef4444;margin-left:4px}
</style>
