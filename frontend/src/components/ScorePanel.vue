<template>
  <div class="panel">
    <h4>🏁 综合评分</h4>
    <template v-if="store.score">
      <div class="score-head">
        <div class="score-total" :class="band">{{ store.score.total }}</div>
        <div class="score-meta">
          <div>执行 #{{ store.score.runId }}</div>
          <div class="frozen">已冻结 · 共 {{ store.score.items.length }} 个环节</div>
        </div>
      </div>
      <div class="score-list">
        <div v-for="it in store.score.items" :key="it.taskId" class="score-row" :class="level(it.score)">
          <span class="s-name">{{ it.name }}</span>
          <span class="s-status">{{ it.status }}{{ it.retries ? ` · 重试${it.retries}` : '' }}</span>
          <span class="s-val">{{ it.score }}</span>
        </div>
      </div>
      <div class="avg-note">总分 = 各环节得分均值 = {{ store.score.total }}</div>
    </template>
    <div v-else class="empty">本次执行尚未完成，评分将在结束时一次性生成…</div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useDAGStore } from '../store/dag'
const store = useDAGStore()
const band = computed(() => {
  const v = store.score?.total ?? 0
  return v >= 85 ? 'good' : v >= 60 ? 'mid' : 'bad'
})
function level(v: number) { return v >= 85 ? 'good' : v >= 60 ? 'mid' : 'bad' }
</script>

<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a}
.panel h4{color:#bb86fc;font-size:12px;margin-bottom:6px}
.score-head{display:flex;align-items:center;gap:10px;margin-bottom:8px}
.score-total{width:52px;height:52px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:800}
.score-total.good{background:#22c55e20;color:#22c55e}
.score-total.mid{background:#fbbf2420;color:#fbbf24}
.score-total.bad{background:#ef444420;color:#ef4444}
.score-meta{font-size:11px;color:#ccc}
.frozen{color:#666;font-size:10px;margin-top:2px}
.score-list{display:flex;flex-direction:column;gap:2px;max-height:220px;overflow-y:auto}
.score-row{display:flex;align-items:center;gap:6px;padding:3px 6px;border-radius:4px;font-size:11px;background:#14142b}
.score-row.good .s-val{color:#22c55e}
.score-row.mid .s-val{color:#fbbf24}
.score-row.bad .s-val{color:#ef4444}
.s-name{flex:1;color:#e0e0e0}
.s-status{color:#888;font-size:10px}
.s-val{font-weight:800;min-width:30px;text-align:right}
.avg-note{margin-top:6px;color:#666;font-size:10px;text-align:right}
.empty{color:#4a5568;font-size:11px;padding:6px 0}
</style>
