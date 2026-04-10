import re
import os
import json
import glob
import time
# 新增逻辑：openai 为可选依赖，便于在无 LLM 环境下运行解析/测试的非 LLM 部分
try:
    from openai import OpenAI
except ModuleNotFoundError:
    OpenAI = None

# 新增逻辑：dotenv 为可选依赖，避免运行环境缺少 python-dotenv 时直接崩溃
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*args, **kwargs):
        return False

# 加载 .env 文件中的 api_key 和 base_url
load_dotenv()

class PostGISFormalParser:
    def __init__(self):
        if OpenAI is None:
            self.client = None
            print("⚠️  [Warn] 'openai' package not installed. LLM parsing will be unavailable.")
        else:
            self.client = OpenAI(
                api_key=os.getenv("api_key"),
                base_url=os.getenv("base_url")
            )
        
        # === 1. XML 结构解析正则 (完全保留) ===
        # 匹配函数条目 (refentry)
        self.re_entry = re.compile(r'<refentry\s+xml:id="(.*?)".*?>(.*?)</refentry>', re.DOTALL)
        # 匹配函数原型 (funcprototype)
        self.re_proto = re.compile(r'<funcprototype>(.*?)</funcprototype>', re.DOTALL)
        self.re_funcdef = re.compile(r'<funcdef>(.*?)<function>(.*?)</function></funcdef>', re.DOTALL)
        self.re_param = re.compile(r'<paramdef>(.*?)</paramdef>', re.DOTALL)
        # 匹配描述 (Description)
        self.re_desc = re.compile(r'<refsection>\s*<title>Description</title>(.*?)</refsection>', re.DOTALL)
        # 匹配标准兼容性 (Standard Compliance)
        self.re_std = re.compile(r'<refsection>\s*<title>Standard Compliance</title>(.*?)</refsection>', re.DOTALL)
        
        # === 2. 示例代码块正则 (完全保留) ===
        # 能够同时抓取 <programlisting> (输入) 和 <screen> (输出/结果)
        self.re_ex_blocks = re.compile(r'<(programlisting|screen)>(.*?)</\1>', re.DOTALL)
        
        # 清除 XML 标签的工具正则
        self.clean_tags = re.compile(r'<[^>]+>')
        
        # === 3. 缺失表与依赖分析（增量增强）===
        # 新增 execution_mode：intra_missing（示例内缺表/缺建表步骤）
        self.allowed_execution_modes = {"safe", "chain", "blocked", "intra_missing"}
        
        # 轻量 SQL 正则：用于后处理补全跨示例依赖信息与统计
        self.re_create_table = re.compile(
            r'\bCREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>(?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+))?)',
            re.IGNORECASE
        )

    def _parse_ex_to_pairs_via_llm(self, raw_ex_text, func_name):
        """
        [增强版] 调用大模型解析原始代码块。
        核心改进：
        1. 识别多个独立示例 (Examples)。
        2. 强制问题包含具体数值 (Specific Values)。
        3. 保留依赖表检测 (Missing Tables)。
        4. [核心优化] 表格数据提取为结构化的键值对列表 (List of Dicts)。
        """
        if not raw_ex_text.strip():
            return []
        
        if self.client is None:
            print(f"      [LLM Error on {func_name}]: OpenAI client not available (missing 'openai' package).")
            return []

        # === 核心 Prompt（增强缺失表分类与结构特征输出）===
        system_prompt = f"""
        You are a generic Data Engineer specialized in PostGIS SQL.
        Your task is to parse raw PostGIS documentation examples into a structured dataset.

        ### Target Function: {func_name}

        The documentation text often contains **multiple distinct, independent examples** (Scenarios).
        You must structure the output hierarchically: **Examples -> Steps**.

        ### Tasks:
        1. **Example Grouping**: Identify distinct scenarios. 
           - Assign an `example_id` (1, 2...) to each scenario.
           - Give a short `name` to the scenario (e.g., "Basic Usage", "Using with 3D Coords").
        
        2. **Step Decomposition**: WITHIN each example, split the code into logical execution steps.
           - Setup (CREATE TABLE/INSERT) -> Operation -> Select.
           - If a query depends on a previous CREATE TABLE *in the same example*, it is a subsequent step.
        
        3. **Question Clarification (CRITICAL)**: 
           - The original example might lack context. You MUST generate a **clear, explicit, self-contained question**.
           - **MANDATORY: EMBED SPECIFIC VALUES**. If the SQL uses specific literals (coordinates, radii, SRIDs, strings, numbers), you **MUST** include them in the question.
           - Do NOT use generic terms like "the point" or "a buffer" if a value exists.

           **Examples of Question Refinement:**
           - *Bad*: "Calculate distance between two points."
           - *Good*: "Calculate the distance between POINT(0 0) and POINT(10 10)."
           - *Bad*: "Buffer the line."
           - *Good*: "Create a buffer of 50 meters around 'LINESTRING(0 0, 10 10)'."
           - *Bad*: "Transform the geometry."
           - *Good*: "Transform the geometry to SRID 4326."

        4. **Execution Mode & Dependencies**:
           - **`safe`**: Runnable immediately (literals, system tables, CTEs).
           - **`chain`**: Runnable ONLY if previous steps *in this specific example* are executed first.
           - **`blocked`**: Relies on external datasets NOT defined in this documentation (e.g., `nyc_streets`).
           - **`intra_missing`**: The step references tables that are NOT created within this example's steps.
             Use this ONLY for **intra-example missing tables** (see missing_tables.missing_type below).
           - **`missing_tables`**: For each missing table, output a detailed object with type labels and schema features.
           
           Missing table types:
           - **external**: the table is not created anywhere in this function documentation examples and looks like an external dataset.
           - **intra_example**: the table is referenced but not created in the SAME example steps.
           - **cross_example**: the table is created in a DIFFERENT example (dependency across examples).
           
           Cross-example dependency fields:
           - If missing_type is cross_example and you can identify the dependent example within THIS function doc, set:
             - dep_scope = "same_func_dep"
             - dep_example_id = the example_id number that creates the table
             - dep_function_id = null
           - If the table seems created in a different function doc (not present in this function's examples), set:
             - dep_scope = "cross_func_dep"
             - dep_function_id = a best-effort guess of the function identifier if visible; otherwise null
             - dep_example_id = best-effort guess if visible; otherwise null
           
           Table schema features (best-effort from SQL usage; null if unknown):
           - has_geometry: true/false/null
           - geometry_column: a column name like "geom"/"the_geom"/null
           - primary_key: a column name like "id"/"gid"/null
        
        5. **Result Extraction (KEY-VALUE PAIRS)**:
           - Extract expected results from comments (`-- Result: ...`) or `<screen>` output blocks.
           - **TABLE DATA TRANSFORMATION**:
             - If the result is an ASCII table (headers + rows), **TRANSFORM IT INTO A LIST OF OBJECTS**.
             - Map the Table Header to the JSON Key, and the Row Data to the Value.
             - Parse numbers as numbers, booleans as booleans, strings as strings.
             
             **Example Transformation:**
             *Raw Input:*
             ```
             +----+----------------+
             | id |      geom      |
             +----+----------------+
             |  1 | POINT(10 10)   |
             +----+----------------+
             ```
             *Expected Output (JSON List):*
             `[{{ "id": 1, "geom": "POINT(10 10)" }}]`
           
           - **SCALAR RESULTS**: If the result is a simple single value (e.g., "t", "105.2"), keep it as a raw value.
           - Set to `null` if completely missing.

        ### Output Format (Strict JSON):
        {{
          "examples": [
            {{
              "example_id": 1,
              "name": "Scenario Name",
              "steps": [
                {{
                  "step_id": 1,
                  "execution_mode": "safe/chain/blocked/intra_missing",
                  "missing_tables": [
                    {{
                      "table": "schema.table_or_table",
                      "missing_type": "external/intra_example/cross_example",
                      "dep_scope": "same_func_dep/cross_func_dep" or null,
                      "dep_example_id": 1 or null,
                      "dep_function_id": "ST_Buffer" or null,
                      "table_features": {{
                        "has_geometry": true/false/null,
                        "geometry_column": "geom" or null,
                        "primary_key": "id" or null
                      }}
                    }}
                  ] or [], 
                  "sql_category": "DQL/DDL/DML",
                  "original_input": "The raw text snippet",
                  "question": "Explicit question with SPECIFIC VALUES included",
                  "sql": "The clean, executable SQL statement",
                  "expected_result": [{{ "col": "val" }}] (for tables) OR "raw_value" (for scalars) OR null
                }}
              ]
            }}
          ]
        }}
        """
        
        user_content = f"Analyze and parse this PostGIS example block:\n\n{raw_ex_text}"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model="gpt-4o",  
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_content}
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.0,
                    timeout=90.0
                )
                res_json = json.loads(response.choices[0].message.content)
                return res_json.get("examples", [])
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"      [LLM Error on {func_name}]: {e} (Retrying {attempt+1}/{max_retries})...")
                    time.sleep(2)
                    continue
                print(f"      [LLM Error on {func_name}]: {e}")
                return []

    def _normalize_table_name(self, table_name):
        if not isinstance(table_name, str):
            return None
        return table_name.strip().strip('"')

    def _normalize_missing_tables(self, missing_tables):
        """
        新增逻辑：统一 missing_tables 的数据形态，兼容旧数据（list[str]）与新数据（list[dict]）。
        返回：list[dict]，每项至少包含 table/missing_type/dep_* / table_features（缺失填 None）。
        """
        if not missing_tables:
            return []
        
        normalized = []
        if isinstance(missing_tables, list):
            for item in missing_tables:
                if isinstance(item, str):
                    t = self._normalize_table_name(item)
                    if not t:
                        continue
                    normalized.append({
                        "table": t,
                        "missing_type": None,
                        "dep_scope": None,
                        "dep_example_id": None,
                        "dep_function_id": None,
                        "table_features": {
                            "has_geometry": None,
                            "geometry_column": None,
                            "primary_key": None
                        }
                    })
                    continue
                
                if isinstance(item, dict):
                    t = self._normalize_table_name(item.get("table") or item.get("name"))
                    if not t:
                        continue
                    features = item.get("table_features") if isinstance(item.get("table_features"), dict) else {}
                    normalized.append({
                        "table": t,
                        "missing_type": item.get("missing_type"),
                        "dep_scope": item.get("dep_scope"),
                        "dep_example_id": item.get("dep_example_id"),
                        "dep_function_id": item.get("dep_function_id"),
                        "table_features": {
                            "has_geometry": features.get("has_geometry"),
                            "geometry_column": features.get("geometry_column"),
                            "primary_key": features.get("primary_key")
                        }
                    })
                    continue
        return normalized

    def _post_process_examples(self, parsed_examples, func_id):
        """
        新增逻辑：对 LLM 产物做轻量纠偏与兼容处理，避免异常值破坏下游验证逻辑。
        """
        if not isinstance(parsed_examples, list):
            return []
        
        fixed = []
        for ex in parsed_examples:
            if not isinstance(ex, dict):
                continue
            steps = ex.get("steps", [])
            if not isinstance(steps, list):
                steps = []
            
            for step in steps:
                if not isinstance(step, dict):
                    continue
                
                mode = step.get("execution_mode")
                if mode not in self.allowed_execution_modes:
                    print(f"      [Warn] Invalid execution_mode '{mode}' in {func_id}, fallback to 'safe'")
                    step["execution_mode"] = "safe"
                
                step["missing_tables"] = self._normalize_missing_tables(step.get("missing_tables"))
            
            ex["steps"] = steps
            fixed.append(ex)
        
        return fixed

    def _index_created_tables_from_dataset(self, dataset):
        """
        新增逻辑：从全量解析结果中提取 CREATE TABLE 目标表，构建 table -> [ {function_id, example_id} ] 索引。
        用于补全 cross_example 的 dep 信息与外部/跨函数依赖的判别。
        """
        index = {}
        for entry in dataset:
            if not isinstance(entry, dict):
                continue
            func_id = entry.get("function_id")
            for ex in entry.get("examples", []) or []:
                if not isinstance(ex, dict):
                    continue
                ex_id = ex.get("example_id")
                for step in ex.get("steps", []) or []:
                    if not isinstance(step, dict):
                        continue
                    sql = step.get("sql") or ""
                    m = self.re_create_table.search(sql)
                    if not m:
                        continue
                    table = self._normalize_table_name(m.group("name"))
                    if not table:
                        continue
                    index.setdefault(table, [])
                    index[table].append({"function_id": func_id, "example_id": ex_id})
        return index

    def _enrich_cross_example_deps(self, dataset):
        """
        新增逻辑：基于 CREATE TABLE 索引，补全 missing_tables 里的 dep_example_id/dep_scope/dep_function_id。
        仅在缺失或明显不一致时填充，不覆盖模型已提供的明确依赖。
        """
        table_index = self._index_created_tables_from_dataset(dataset)
        
        for entry in dataset:
            if not isinstance(entry, dict):
                continue
            func_id = entry.get("function_id")
            for ex in entry.get("examples", []) or []:
                if not isinstance(ex, dict):
                    continue
                for step in ex.get("steps", []) or []:
                    if not isinstance(step, dict):
                        continue
                    mts = step.get("missing_tables") or []
                    if not isinstance(mts, list):
                        continue
                    for mt in mts:
                        if not isinstance(mt, dict):
                            continue
                        table = self._normalize_table_name(mt.get("table"))
                        if not table:
                            continue
                        
                        candidates = table_index.get(table) or []
                        if not candidates:
                            continue
                        
                        # 如果模型标了 external 但我们能在其他示例中找到 CREATE TABLE，升级为 cross_example
                        if mt.get("missing_type") == "external":
                            mt["missing_type"] = "cross_example"
                        
                        if mt.get("missing_type") != "cross_example":
                            continue
                        
                        if mt.get("dep_scope") and (mt.get("dep_example_id") is not None or mt.get("dep_function_id")):
                            continue
                        
                        same_func = next((c for c in candidates if c.get("function_id") == func_id), None)
                        if same_func:
                            mt["dep_scope"] = "same_func_dep"
                            mt["dep_example_id"] = same_func.get("example_id")
                            mt["dep_function_id"] = None
                        else:
                            mt["dep_scope"] = "cross_func_dep"
                            mt["dep_function_id"] = candidates[0].get("function_id")
                            mt["dep_example_id"] = candidates[0].get("example_id")

    def parse_single_file(self, file_path):
        """解析单个 XML 文件"""
        results = []
        
        # === 提取章节信息 (文件名去后缀) ===
        file_name = os.path.basename(file_path)
        chapter_info = os.path.splitext(file_name)[0]  # 例如: "ST_Buffer"

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content_all = f.read()
        except Exception as e:
            print(f"Read Error: {file_path} - {e}")
            return []

        entries = self.re_entry.findall(content_all)
        for func_id, body in entries:
            print(f"  -> Parsing Function: {func_id} (Chapter: {chapter_info})")

            # 1. 提取函数定义 (Signatures)
            func_defs = []
            for proto in self.re_proto.findall(body):
                fdef = self.re_funcdef.search(proto)
                ret_type = self.clean_tags.sub('', fdef.group(1)).strip() if fdef else ""
                f_name = fdef.group(2).strip() if fdef else func_id
                p_list = [self.clean_tags.sub('', p).strip() for p in self.re_param.findall(proto)]
                
                func_defs.append({
                    "function_name": f_name,
                    "return_type": ret_type,
                    "arguments": p_list,
                    "signature_str": f"{f_name}({', '.join(p_list)})"
                })

            # 2. 提取描述 (Description)
            desc_m = self.re_desc.search(body)
            description = " ".join(self.clean_tags.sub('', desc_m.group(1)).split()) if desc_m else ""

            # 3. 提取标准兼容性 (Compliance)
            std_m = self.re_std.search(body)
            std_compliance = self.clean_tags.sub('', std_m.group(1)).strip() if std_m else "N/A"

            # 4. 提取并处理示例 (Examples)
            raw_ex_texts = [m[1] for m in self.re_ex_blocks.findall(body)]
            full_raw_ex = "\n\n".join(raw_ex_texts)
            
            parsed_examples = []
            if full_raw_ex.strip():
                # 注意：这里返回的是 List[Example] 而不是 List[Step]
                parsed_examples = self._post_process_examples(
                    self._parse_ex_to_pairs_via_llm(full_raw_ex, func_id),
                    func_id
                )

            # 5. 组装最终对象
            results.append({
                "function_id": func_id,
                "chapter_info": chapter_info,
                "source_file": file_name,       # <--- 保持 source_file
                "function_definitions": func_defs,
                "description": description,
                "standard_compliance": std_compliance,
                "examples": parsed_examples 
            })
        return results

    def batch_process(self, input_dir, output_file):
        """批量处理目录下所有 XML 文件"""
        all_data = []
        if not os.path.exists(input_dir):
            print(f"Error: Directory {input_dir} does not exist.")
            return

        files = glob.glob(os.path.join(input_dir, "*.xml"))
        
        if not files:
            print(f"Warning: No XML files found in {input_dir}")
            return

        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        print(f"🚀 Starting extraction from {len(files)} files...")
        
        for i, p in enumerate(files):
            print(f"[{i+1}/{len(files)}] Processing: {os.path.basename(p)}")
            all_data.extend(self.parse_single_file(p))

        # 新增逻辑：批量解析完成后，基于 CREATE TABLE 索引补全跨示例依赖信息
        self._enrich_cross_example_deps(all_data)

        # === 统计有效数据 (适配新的 examples -> steps 结构) ===
        valid_data = [d for d in all_data if d.get('examples')]
        
        total_examples = sum(len(d['examples']) for d in valid_data)
        total_steps = 0
        missing_table_counts = {"external": 0, "intra_example": 0, "cross_example": 0, "unknown": 0}
        missing_total = 0
        for d in valid_data:
            for ex in d['examples']:
                total_steps += len(ex.get('steps', []))
                for step in ex.get('steps', []):
                    mts = step.get("missing_tables") or []
                    if not isinstance(mts, list):
                        continue
                    for mt in mts:
                        missing_total += 1
                        if isinstance(mt, dict):
                            t = mt.get("missing_type") or "unknown"
                        else:
                            t = "unknown"
                        if t not in missing_table_counts:
                            t = "unknown"
                        missing_table_counts[t] += 1
        
        # 写入结果
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(valid_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Extraction Finished!")
        print(f"   Output File: {output_file}")
        print(f"   Total Functions Parsed: {len(all_data)}")
        print(f"   Functions with Examples: {len(valid_data)}")
        print(f"   Total Independent Examples: {total_examples}")
        print(f"   Total SQL Steps Extracted: {total_steps}")
        print(f"   Missing Tables (Total): {missing_total}")
        print(f"   Missing Tables by Type: external={missing_table_counts['external']}, intra_example={missing_table_counts['intra_example']}, cross_example={missing_table_counts['cross_example']}, unknown={missing_table_counts['unknown']}")

if __name__ == "__main__":
    # === 配置路径 ===
    INPUT_DIR = "./xml_data" 
    OUTPUT_FILE = "extract_result/postgis_extracted2.json"

    parser = PostGISFormalParser()
    parser.batch_process(INPUT_DIR, OUTPUT_FILE)
