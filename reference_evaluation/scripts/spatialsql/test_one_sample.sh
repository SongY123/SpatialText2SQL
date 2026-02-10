#!/bin/bash
# SpatialSQL 小样本测试：从 dataset1_ada 挑 3 条做推理评估，验证流程可通
set -e
cd /home/xutengfei/reference_evaluation

# 备份原始文件
cp data/preprocessed/spatialsql_pg/splitdataset1_ada_with_schema.json \
   data/preprocessed/spatialsql_pg/splitdataset1_ada_with_schema.json.bak

# 临时仅保留前 3 条用于测试（快速验证）
conda run -n text2sql python - <<'PY'
import json
with open('data/preprocessed/spatialsql_pg/splitdataset1_ada_with_schema.json','r',encoding='utf-8') as f:
    data=json.load(f)
sample=data[:3]
with open('data/preprocessed/spatialsql_pg/splitdataset1_ada_with_schema.json','w',encoding='utf-8') as f:
    json.dump(sample,f,ensure_ascii=False,indent=2)
print(f'Reduced to {len(sample)} samples for quick test')
PY

# 执行推理评估（仅 base 配置，单模型）
echo "Running inference+evaluation on 3 samples..."
conda run -n text2sql python src/main.py --inference --evaluate \
  --dataset spatialsql_pg \
  --models qwen2.5-coder-7b \
  --configs base

# 恢复原始文件
mv data/preprocessed/spatialsql_pg/splitdataset1_ada_with_schema.json.bak \
   data/preprocessed/spatialsql_pg/splitdataset1_ada_with_schema.json

echo "Test completed! Check results/predictions/ and results/evaluations/"
