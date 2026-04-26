# SpatialText2SQL 项目执行计划（按流程图更新）

## 0. 更新说明

基于最新流程图，本计划已从“仅前置合成”升级为“全链路对齐 + 当前负责模块落地”版本：

- 全链路模块：`Table Canonicalization -> DB Synthesis -> SQL Synthesis -> NL Synthesis -> Quality Control`
- 你当前优先负责：`Table Canonicalization` 与 `DB Synthesis`
- 下游（SQL/NL/质量）先做接口对齐，不在本阶段实现

---

## 1. 流程图拆解（模块与输入输出）

## 1.1 Table Canonicalization（你负责，P0）

子模块：

1. `Table Normalization`
2. `Spatial Column Identification`
3. `Thematic Labeling`
4. 产出 `Canonical Spatial Tables`

输入：

- 各城市原始空间表（Raw Spatial Tables）
- 表元数据（name/summary/description/tags/columns）

输出（建议文件）：

- `canonical_tables.jsonl`：每表一条标准记录
- `canonical_schema_summary.csv`：字段类型与空间字段统计

---

## 1.2 DB Synthesis（你负责，P0）

子模块：

1. `Relation Graph Construction`
2. `Table Relation Discovery`
3. 采样生成 `Databases`（合成库）

输入：

- `canonical_tables.jsonl`

输出（建议文件）：

- `relation_graph_<city>.json`
- `sampled_databases.jsonl`

---

## 1.3 SQL Synthesis（他人主导，你做接口，P1）

子模块：

- `Spatial Intent`（range/join/nearest neighbor/distance/aggregation）
- `Difficulty Control`（表数、join数、spatial op复杂度）

输入：

- `sampled_databases.jsonl`

输出接口约定（先定义不实现）：

- `sql_tasks.jsonl`（含模板类型、难度标签、目标SQL）

---

## 1.4 NL Synthesis（他人主导，你做接口，P1）

子模块：

- `Linguistic Style`（conversational/formal/direct）
- `Relation Phrasing`（within/near/contained等）

输入：

- `sql_tasks.jsonl`

输出接口约定（先定义不实现）：

- `nl_sql_pairs.jsonl`

---

## 1.5 Quality Control（他人主导，你做接口，P1）

子模块：

- `Execution Validation`
- `Self-Consistency`

输入：

- `nl_sql_pairs.jsonl`
- 目标 spatial database

输出接口约定（先定义不实现）：

- `quality_report.json`
- `accepted_pairs.jsonl`

---

## 2. 你当前阶段的明确目标（本周）

只做两件事：

1. 把原始表标准化为 `Canonical Spatial Tables`
2. 从 canonical 表构建关系图并采样合成数据库

不做：

- SQL 生成
- NL 生成
- 执行验证与一致性过滤

---

## 3. 关键口径（必须先统一）

## 3.1 术语

- `special field` 统一写作 `spatial field`
- `dataset_uid` 统一格式：`city:dataset_id`

## 3.2 字段类型归一化

最小集合：

- `text`
- `bigint`
- `double`
- `boolean`
- `date`
- `timestamp`
- `geometry`
- `json`
- `other`

## 3.3 空间字段识别

分三层：

1. 名称规则：`geom/geometry/the_geom/lat/lon/wkt/...`
2. 内容规则：WKT/GeoJSON/坐标对
3. 元信息：geometry subtype + CRS（不可解析则 `unknown`）

---

## 4. 数据结构设计（落地前必须冻结）

## 4.1 canonical_tables 记录结构

每条至少包含：

- `dataset_uid`
- `city`
- `table_name`
- `summary`
- `raw_columns`
- `normalized_columns`（name + normalized_type）
- `spatial_columns`
- `spatial_meta`（geometry_type/crs）
- `thematic_labels`（站点标签 + 14类映射）

## 4.2 relation_graph 结构

- 节点：`dataset_uid`
- 边：
  - `semantic_sim`：由 name+summary embedding 相似度得到
  - `spatial_related`：空间字段/geometry相关性得到
- 边字段：`src`, `dst`, `score`, `edge_type`

## 4.3 sampled_databases 结构

- `sample_id`
- `city`
- `seed_table`
- `tables`（3~10）
- `edges`
- `walk_trace`
- `stats`（avg_similarity/jump_count/spatial_coverage）

---

## 5. 核心算法计划（你负责部分）

## 5.1 Relation Graph Construction

1. 对同城表构建 embedding（name 与 summary 分别编码后 concat）
2. 计算两两相似度
3. 阈值建边：`score >= T`
4. 增加 `spatial_related` 边（可用规则得分）

建议默认参数：

- `T = 0.55`
- 每节点可设 `top_k` 保底（如 8）

## 5.2 Table Relation Discovery + 随机游走采样

1. 随机选 seed
2. 按边权概率游走扩展
3. 以概率 `p_jump` 跳转到非邻居节点（增强跨场景）
4. 去重并控制规模到 3~10
5. 输出样本与 trace

建议默认参数：

- `p_jump = 0.25`
- `sample_size_range = [3, 10]`
- `sample_size_mean = 6`
- `max_jump_per_walk = 2`
- `random_seed = 42`

---

## 6. 与下游模块的接口约束（提前避免返工）

对 `SQL Synthesis` 的接口要求：

- 每个 sample 必须给出清晰 schema（表、字段、主键候选、空间字段）
- 必须包含可用于难度控制的统计：`n_tables`, `n_joins_possible`, `spatial_op_candidates`

对 `NL Synthesis` 的接口要求：

- 保留关系语义标签：`within`, `near`, `intersect`, `contain` 等可表达短语

对 `Quality Control` 的接口要求：

- 每个样本可还原建库过程（trace 可追溯）
- 每条候选可回溯来源样本 `sample_id`

---

## 7. 验收标准（你本阶段）

## 7.1 Canonicalization 验收

- 空间字段识别准确（抽样人工核查）
- 字段类型归一化覆盖率高（`other` 比例可控）
- 每表记录完整，无关键字段缺失

## 7.2 DB Synthesis 验收

- 每个样本表数都在 3~10
- 每样本至少 1 张含 spatial 字段的表
- 样本总体重复率可控
- 可解释性满足：每样本有完整 walk trace

---

## 8. 风险与预案

风险 1：描述质量差导致语义边不稳  
预案：拼接字段名摘要与标签增强文本

风险 2：图过稀/过密  
预案：阈值 + top_k 双机制调控

风险 3：跳转过多导致主题漂移  
预案：限制 `max_jump_per_walk`

风险 4：计算成本高  
预案：先 ANN 召回候选，再精算相似度

---

## 9. 3 天对齐排期（不改代码）

Day 1：

- 冻结术语、空间识别规则、字段归一化字典
- 冻结 canonical 记录结构

Day 2：

- 冻结图构建与采样策略
- 冻结默认参数

Day 3：

- 冻结验收标准与接口约束
- 输出评审版（可进入实现）

---

## 10. 你的行动清单（Checklist）

- [ ] 确认 `special` 全部改写为 `spatial`
- [ ] 完成 canonical 字段清单与示例
- [ ] 完成 spatial 识别规则文档
- [ ] 完成字段归一化字典文档
- [ ] 完成 relation graph 口径文档（边类型与阈值）
- [ ] 完成随机游走参数文档（含 jump 策略）
- [ ] 完成与 SQL/NL/Quality 的接口字段文档
- [ ] 完成验收指标表并组织评审

---

## 11. 待拍板决策（开会直接问）

1. `p_jump` 用 `0.2`、`0.25` 还是 `0.3`
2. 样本规模均值取 `6` 还是 `7`
3. `thematic label` 第一版只记录，还是参与边权计算

# 数据库合成前置阶段 - 详细执行计划

## 0. 文档目的

这份计划用于把会议口述需求转换成可执行任务清单，先完成方案对齐与验收标准设计。

---

## 1. 项目目标与边界

### 1.1 目标

构建一个“可控、可解释、可复现”的数据库合成前置流程，核心是：

1. 识别并结构化每张表的信息（尤其 spatial 字段）
2. 构建城市内表相似图
3. 基于随机游走采样得到 3~10 张表规模的 schema 子图
4. 为后续 SQL/NL 生成提供输入

### 1.2 当前不做

- 不做 SQL 样本生成
- 不做 NL 样本生成
- 不做质量过滤器实现
- 不做大规模超参搜索

---

## 2. 术语统一（避免沟通歧义）

- `special field` 统一解释为 `spatial field`
- `source table`：从各城市 open data 抓取或落盘后的原始表
- `table profile`：每张表的结构化画像（供构图和采样）
- `similarity graph`：同一城市内表之间的关系图
- `sampled schema`：随机游走得到的候选数据库子图

---

## 3. 产物定义（先定输出，再实现）

### 3.1 表级画像：`table_profile`

每条记录至少包含：

- `dataset_uid`（建议格式：`city:dataset_id`）
- `city`
- `table_name`
- `summary/description`
- `raw_columns`（原始字段列表）
- `normalized_type_summary`（字段类型归一化统计）
- `spatial_columns`（空间字段名列表）
- `spatial_meta`（几何类型、CRS/坐标系、解析来源）
- `labels`（站点标签 + 内部语义标签）
- `taxonomy_hint`（可选：14 类或 L3 候选）

### 3.2 城市级关系图：`similarity_graph`

- 节点：`dataset_uid`
- 边：`(u, v, score, edge_type)`
- `edge_type` 至少区分：
  - `semantic_sim`：文本语义相似边
  - `spatial_related`：空间字段相关边（可选增强）

### 3.3 采样库清单：`sampled_schema`

每个样本建议包含：

- `sample_id`
- `city`
- `seed_table`
- `tables`（3~10 张）
- `edges`（样本中被采用的边）
- `walk_trace`（每一步是“按相似边走”还是“随机跳转”）
- `stats`（均值相似度、跨场景比例、空间字段覆盖率）

---

## 4. 任务分解（按阶段执行）

### 阶段 A：口径冻结（优先级 P0）

#### A1. 空间字段识别规则冻结

- 字段名规则：`geom`、`geometry`、`lat/lon`、`wkt` 等
- 内容规则：WKT / GeoJSON / 坐标列组合识别
- CRS 规则：可解析则记录，无法解析标记 `unknown`

#### A2. 字段类型归一化规则冻结

最小可用集合建议：

- `text`
- `bigint`
- `double`
- `boolean`
- `date/timestamp`
- `geometry`
- `json`（若存在）
- `other`（兜底）

#### A3. 相似图构建口径冻结

- 输入文本：`table_name` + `summary/description`
- 向量策略：分别 embedding 后 concat
- 相似度：cosine
- 建边条件：`score >= T`

交付物：一页《口径说明》文档（可直接用于组内确认）。

---

### 阶段 B：采样策略设计（优先级 P0）

#### B1. 基础随机游走

- 在城市内图中选 `seed_table`
- 按邻边概率前进（边权归一化）
- 去重节点，避免无限回环
- 直到达到目标表数

#### B2. 跳转策略（增强多样性）

- 每一步以概率 `p_jump` 执行“跳转到非邻近节点”
- 跳转来源可先用“同城随机节点”简化
- 限制单次游走跳转上限，防止语义漂移

#### B3. 样本规模策略

- 目标范围：3~10 张表
- 建议分布：截断正态（先固定均值，不做搜索）

交付物：一页《采样策略说明》+ 参数默认值表。

---

### 阶段 C：验收标准设计（优先级 P0）

#### C1. 结构正确性

- 所有样本表数都在 3~10
- 每样本至少含 1 张有 spatial 字段的表（可配置）
- 无空样本、无全重复样本

#### C2. 质量指标

- 样本内平均相似度（越高越同场景）
- 跨场景混入比例（受 `p_jump` 控制）
- 表覆盖率（城市内多少表被采到）

#### C3. 可解释性

- 每个样本可追溯 walk trace
- 能解释每条边的来源（阈值边/跳转边）

交付物：《验收指标与阈值表》。

---

### 阶段 D：风险预案（优先级 P1）

#### 风险 1：描述文本质量差，语义边不稳定

预案：

- 拼接字段名摘要增强文本
- 引入标签作为辅助特征

#### 风险 2：图过稀或过密

预案：

- 过稀：降低阈值或加 top-k 保底边
- 过密：提高阈值并限制最大邻居数

#### 风险 3：跳转过多导致样本失焦

预案：

- 降低 `p_jump`
- 设置“每个样本最多跳转次数”

#### 风险 4：计算量高（n^2）

预案：

- 用 ANN 做候选近邻，再精算相似度

---

## 5. 默认参数（先定 baseline，后续再调）

以下为建议值，可在小规模试运行后微调：

- `similarity_threshold (T) = 0.55`
- `jump_probability (p_jump) = 0.25`
- `sample_size_range = [3, 10]`
- `sample_size_mean = 6`（截断正态）
- `max_jump_per_walk = 2`
- `random_seed = 42`

---

## 6. 执行时间表（不改代码阶段）

### Day 1：需求口径对齐

- 完成术语统一
- 完成 spatial 识别与类型归一化规则草案
- 完成构图输入字段确认

### Day 2：采样方案对齐

- 完成随机游走+跳转策略文档
- 完成默认参数定稿

### Day 3：验收与风险对齐

- 完成验收指标表
- 完成风险预案
- 输出“可进入实现”的评审版本

---

## 7. 你本周的具体待办清单（Checklist）

- [ ] 整理并确认 spatial 字段识别规则（字段名+内容+CRS）
- [ ] 整理并确认字段类型归一化字典
- [ ] 明确 table profile 输出字段与格式
- [ ] 明确 similarity graph 的边定义与阈值
- [ ] 明确 random walk 采样逻辑（含去重/回环处理）
- [ ] 明确 jump 机制（概率、上限、目标节点规则）
- [ ] 明确样本规模分布（3~10）与默认参数
- [ ] 明确结构/质量/可解释性验收指标
- [ ] 输出一版评审文档，等待进入代码实现阶段

---

## 8. 需要你最终拍板的 3 个决策

1. `p_jump` 默认取 `0.2` 还是 `0.3`（建议先 `0.25`）
2. 样本规模均值取 `6` 还是 `7`
3. 14 类标签在第一版是否参与边权，仅记录还是参与计算

---

## 9. 下一步（收到确认后）

待你确认本计划后，再进入“实现阶段计划”：按模块拆分为 profile 构建、图构建、采样器、评估器四个子任务并开始代码开发。
好，我**直接把所有“人名+时间”的内容全部删掉**，只留纯会议内容，一字不改、完整输出给你：

---

嗯，行，没问题，基本的活儿就是统计这个 source 的表，然后写爬虫对吧？
啊，然后这个弄完了。然后后续的话我这边的一个任务就是我把整个框架图还没画完，还有最后一块，然后具体的一个设计给设计完了，然后我先跟你说一下，因为跟腾飞那边跑下其他的一些实验，可能关系不大，我就提前跟你说了，然后先说一下大概的一个思路啊，合成的思路啊，其实跟那个比较像。然后首先第一步我们有一些爬下来的数据表，对吧，就是 row special database 呃，row special tables 然后它可能会有各种各样的一些字段，有那个 gom 或者说 coordinate 就坐标系的两类的那种，我之前让你统计的时候把它算做一列。

嗯，是的。

啊，然后你首先你需要做的是啊，你需要去识别它的一些那个，special column。

对吧。它有一些 coordinates，比如说 gym multipolygot，或者说 line string 那些东西你都把它识别出来，然后把它坐标系。如果是 geo string 呃 geo json 的话，你把它的坐标系解析出来。

然后那我之前跟你说的，你需要给他们打一些标签，对吧，他本身会有一些标签，但是我后来想了一下那个，其实给他们稍微分一些标签也是。

可以的，他首先他自己每个数据集都有一些标签，但那些标签相当于是每个网站都不太一样，对吧。然后另外的话，我们可以给他分到 14 个类里边的其中一个，这个具体的一个分类方法其实不太重要，因为后来我设计了一下这一块，重要性不大啊，我一会儿跟你讲怎么回事？

啊，然后呢，这个其实就是相当于凑一个供应点，把上面给凑满了。然后接下来我会出现一个那个 special 的一个 table，对吧，然后它有我的一些相关的一些字段，然后有一些我的那个。

西欧的一些字段有一些其他的字符串啊，数字啊，然后这个字符串数字，我建议你这边直接把它们映射为某几类基类，然后比如说字符串的话，它有很长的字符串，有很短的字符串，你统一用一类字符串。

一类 pg 的字符串给它进行一个表示，就不需要做那么多复杂的匹配，比如说我既有 string 我不太知道那个 pg 的语法，我没查啊。

假设字符串既有 string 有挖叉，然后那我可能就定一个 string 就可以了，因为我可能也不需要设置它的一个长度啊，那如果说是整形的话，我就全给它设置成 long，我就不设置成 int，我假设我的 long 都能包括，对吧，我也不需要搞那么多的一个复杂的一些东西，然后，呃，至于。

这个 go 字段的话可能会涉及到一些小的 trick，因为在 pg 里面它其实有很多的类型啊，那个叫什么 special 字段它有很多的类型，它需要做一些转化，然后。

这一块可能是需要做一些小的变化，因为为了我们 SQL 的一个复杂度，多样性，你可能比如说把其中的一个类型转成另一个 geometry 的一个类型，然后这样可能会提升我们整体合成 SQL 的一个质量。

啊，然后其他的就你到时候就看了，然后最后抽取出这样的一个表出来，里面首先有 table name ，对吧。table name 叫 name 可能也不太好。

我直接叫 poi 吧！

假设这个表是一个叫 POI 的一个字段表啊，一个 POI 的字段啊，一个 POI 的表，然后它是属于哪个 city 的？它的 schema 是什么？然后它有一些网站上你爬的那些 description，然后它那个 special 的 field 是什么？然后还有一些 labels ，包括这个 semantic labeling 做的这个 semantic 的 labels labels，包括它自己网站的一些字段，对吧，都有了。然后根据这些字段呢，我们其实在同一个城市，我们要算不同的表之间的一个关联性。

其实本质上就是算一个相似度了啊，你把它的那个 table name 把它的那个 summary，然后做一个怎么说，做一个，呃。

叫什么是呃，用 embedding 去表示，然后你这个 embedding 的话，我一会儿给你，你就直接用这个就行了啊，因为这个效果还可以，然后模型也比较小。

Embedding.

对，你就你就直接用这个吧，我之前用的是这个。你就直接做个 concat，然后把表名和。那个。

把表明和他的 description，然后做一个那个向量表示，然后向量表示，之后把它进行一个 concat 啊，分别做向量表示，分为把表名做向量表示，把 summary 做向量表示。

然后做 embedding 之后，把他们这两个向量表做一个 concat，然后把所有的表假设我你统计的这个表最多的一个应该是 200 多是吧？呃，最多的一个表是 292 那你把 292 张表然后全横着和纵着，然后两两之间计算它们的一个相似度。

然后基于这个相似度，你可能会需要设计设计，呃设计一个阈值，这个阈值怎么说呢，它可能是呃。

这个就看你最终的一个设计了，比如说我设计这个相似度阈值大于某一个值，那他们可能最终会构成一个网络，对吧，我两个表之间有关联性，我就给他们连一条边。

然后基于这个连的边，呃，我可能会有一些随机游走我，我简简单来说，我就做一些随机游走，我从一个点出发，然后以我们之间相似性的那个概率相似性的那个概率的值。

然后进行一个随机随机游走，假设我这有两条边，我走这条边的概率是 0.5 走这条边的概率是 0.4 那我就 50%的概率走这儿 40%的概率走这儿。

然后这样的话，我其实会可以呃，随机的构造出一些啊，局部叫类似于叫 subgraph，对吧，我们这个 sub sub graph ，然后你比如说设计一个 3.10 的一个。

范围就假设我的一个库里边所有的表的数量都是 3.10 然后他们这个数量的话符合一个正态分布的一个情况。那我。

随机游走的话，我会限制他们之间的一个步数是 3.10 呃，三到呃呃，步数可能就走两 2.8 啊 2.9 步，因为我就限制他走 10。

失踪啊，当然可能会有一些回环的一些情况，你反正如果遇到回环，或者已经在你这个候选的节点里面的话，那我就直接接着再往下随机游走。但是我需要注意的一点是。

嗯，这块你可能不光是需要，就是基于它的一个相似度走，因为是，就像我们上次说的，我两个之间如果没有相似度，但他们本身因为 special 有一些关联性，他们可能也会产生一些其他的 special 的关联。

对吧啊，为什么会这样？就是我们上次说的我两个不同的场啊，不同场景下的数据反而可能会产生 special 的关联。所以我建议这一块在具体设计的时候不能完全说，我就按照。

这个他们上次做的一个概率，在这随机游走，我可能会需要设计一个，比如说额外的概率，它有一定的概率会跳脱出你们当前的一个。

节点直接跳到其他的节点去，随机一个节点都可以，然后通过增加一些随机性的方，呃，随机性的策略，然后。

来构造我们的这个 schema。你大概懂我的意思了吗？

嗯，大概是懂的。

对，就是就是我刚刚说的把他们 table name sum 呃， summary 然后做两两两个表，两个表，同一个城市下面两个表，两表之间的一个相似度的一个呃。

图的构建构建完了之后，然后你设定一个阈值超过这个阈值的就代表他们有一条连线对吧，然后我把这个整个图相当于先建起来，图建起来之后，我针对我，首先我要采样得到一个表。

然后采样得到一个库，那我从一个表出发，那其实与我随机游走，假设我一开始随机游走都是跟我相关的一些表，那我采集到的这些都是跟我相似场景的一些数据，对吧？

然后我采集相似的场景的数据 OK 这没问题，但是我希望引入一些额外的那个额外的什么情况呢？就是跳脱出我们当前这个场景的一些表格，我也想纳入进来，因为他们可之间可能还会。

有一些 special 的关联，尽管他们的业务场景不一样啊，所以说你这边在做，我建议你这边比如说设置一个额定的阈值，比如说你就设置成 0.3，那我有 0.3 的概率，这个其实是一个超参了。

但是至于要不要对它进行超参分析，后面再说了，我觉得你就设置设计成一个固定的值，到时候比如说 0.3 的概率或者 0.2 的概率，我会跳到与它不相关的一个表里边去。

啊，这样，然后把我的这个 schema 构建出来，然后整体的 schema 的数量一定要符合，呃，一定要我觉得我建议可以符合一个那个。

正态分布啊，就是 3.13.10 的一个正态分布，你如果一个库里面一张表也不太现实，就 3.10 张表，我觉得可能是一个比较合理的值。

因为你呃，你看这里面的平均的库的数量基本上也是 10 左右嗯 3.13.12 都行吧，我觉得啊。

OK 行行反正大概就这样，然后嗯，这步你基本都清楚了吗？就是你做数据库合成这一块。

嗯，行，到时候不会再问吧！

对行行行，然后还有就是刚刚说的那个 jim 的字段啊，我建议你是在构造 db 的时候，你在里边啊，不是在这一步加随机性，而是在合成。

数据的啊，或者数据库的时候给他们加随机性，因为我一个表在这个库里边，可能是某种那个 geometry 的表现形式，呃，某种 geometry 的类型。然后我在另外一个库里面，那可能是另外一个 geometry 的一个类型。这样的话其实相对的是增加了一些随机性，然后多了更多的 geometry 的字段的覆盖，可能鲁棒性会更好一些。

啊。好吧。

行，那个我觉得你那边可能嗯，暂时做这么多就 OK 了啊，然后后面的 SQL 生成那一块和那个 nl 的生成那一块以及 quality 的过滤，嗯，回头都我来写吧啊！

