# 渡轮结算 · Iceberg v2 票号删除核对

用于核对湖仓源快照中，某航次日分区里的票号是否已被 **Iceberg v2 位置删除**
（positional delete）标记。网页接收有效文件清单和对应 Parquet 副本，输入整数
票号，逐条给出原文件、从 0 起算的文件内行号、是否已删除以及命中的删除文件。

## 快速启动

```bash
docker compose up --build
```

打开 <http://localhost:8000>，点 **“载入样例”** 即可运行内置演示。接口健康检查：

```bash
curl http://localhost:8000/healthz
```

停止：

```bash
docker compose down
```

## 网页用法

1. 上传 **有效文件清单**（CSV 或 JSON）。
2. 上传清单中所有 `data` / `position_delete` 文件的 **Parquet 副本**，可多选。
   文件名必须与清单的 `uploaded_filename` 一致。
3. 填写整数票号（逗号、空格、分号或换行分隔），以及票号列名（默认
   `ticket_no`）。
4. 开始核对。

上传文件只保存在一次请求的临时目录中，请求结束后删除；不会按用户文件名
拼接服务器路径。

### 清单格式

CSV（推荐）：

```csv
original_path,uploaded_filename,file_type,data_sequence_number
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/data/data-0001.parquet,data-0001.parquet,data,7
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/delete-pos/delete-pos-0001.parquet,delete-pos-0001.parquet,position_delete,12
```

JSON 也支持，顶层可以是数组，或包含 `files` / `entries` 数组的对象：

```json
[
  {
    "original_path": "s3://.../data/data-0001.parquet",
    "uploaded_filename": "data-0001.parquet",
    "file_type": "data",
    "data_sequence_number": 7
  }
]
```

列名可使用以下别名：

| 含义 | 可用列名 |
| --- | --- |
| 原始路径 | `original_path`, `original_file_path`, `source_path`, `path` |
| 上传文件名 | `uploaded_filename`, `uploaded_file`, `filename` |
| 文件类型 | `file_type`, `content`, `type` |
| 数据序列号 | `data_sequence_number`, `sequence_number`, `seq` |

`file_type` 可选值：

| 值 | 行为 |
| --- | --- |
| `data` / `0` | 数据文件，参与扫描 |
| `position_delete` / `positional_delete` / `1` / `delete` | 位置删除文件，参与匹配 |
| `equality_delete` / `2` | 等值删除，**忽略**；不要求上传 Parquet 副本 |

只处理位置删除。位置删除 Parquet 必须包含 `file_path`（字符串）和 `pos`
（整数）两列，其他列（例如 Iceberg 可选的 `row`）会被忽略。

## 核对规则

- **批量行组跳读**：一次输入的多个票号作为一批核对。对每个数据文件的每个
  行组，只有 min/max 统计能排除 **整批所有票号** 时才跳过该行组；否则该行组
  的票号列只读取一次，一次性匹配整批票号。统计缺失（`statistics` 为 `None`
  或没有 min/max）时必然读取该行组。扫描完成后不保留读到的 Arrow 列数据，
  只保留逐票命中行号和诊断所需的共享行组元数据。
- **逐票诊断**：结果页只携带一份与票号无关的共享行组元数据。展开某个票号的
  诊断时，前端按 **当前票号** 的统计范围重新判断每个行组是否可跳过，不再读取
  Parquet。为其他票号读取过的行组，如果统计排除了当前票号，仍标记为“跳过”，
  不会算作当前票号的必读行组。
- **原始行号**：行号始终是数据文件中从 0 起算的原始行号 =
  前面所有行组的行数之和 + 行组内原始下标。筛选或只读取某个行组都不会重置、
  压缩或重新编号；逐票的命中行号与删除依据会分别保留。
- **位置删除匹配**：删除记录必须同时满足
  1. 删除记录的 `file_path` 精确等于清单中的 `original_path`；
  2. `pos` 精确等于该命中行的从 0 起算的行号；
  3. 删除文件的数据序列号 **不小于** 数据文件的数据序列号。

  不满足序列号条件的候选删除文件会显示为“被规则忽略的删除记录”，但不会
  把票判成已删除。
- **全有或全无**：清单中的任何一个 `data` / `position_delete` 副本缺失，
  或任一文件打不开、行组读取失败，整次核对都会中止并明确列出文件名，不显示
  任何票号的部分结论。未出现在清单中的上传文件会被忽略并给出警告。

## 内置样例

`sample/` 中的演示数据由 `tools/make_sample.py` 生成：

- `data-0001.parquet`：5 个行组，每个 20 行。
  - 行组 0–3 的票号范围分别是 1000–1019、2000–2019、3000–3019、4000–4019。
  - 票号 **4242** 在第 5 个行组（索引 4），全局行号 **99**。
  - 批量核对样例票号时，行组 0 因包含 1003、行组 4 因包含 4218/4242
    各读取一次；行组 1–3 被整批统计排除，直接跳过。展开 4242 的诊断时，
    行组 0 仍按 4242 的统计判为“跳过”，并注明它是为其他票号读取的。
  - 读取第 5 个行组后仍按原始行号 99 命中 `delete-pos-0001.parquet`。
- `data-0002.parquet`：写入时关闭了列统计，票号 **8888** 在第 2 个行组
  （全局行号 **19**）。因为没有统计，两个行组都只各读取一次，随后命中
  `delete-pos-0002.parquet`。
- `delete-pos-stale-0001.parquet`：也指向 4242 / 行 99，但数据序列号是 6，
  小于数据文件的 7，因此被忽略。
- 清单中的 `equality_delete` 项没有上传副本，核对仍正常完成，因为它被忽略。

样例默认票号：`4242, 1003, 4218, 8888, 9999`。其中 1003 已删除，4218
存在但未删除，9999 不存在。

## 本地开发与测试

需要 Python 3.11+、Flask、PyArrow。

```bash
python3 -m pip install -r requirements.txt
python3 app.py                     # http://localhost:8000
python3 -m pytest -q               # 42 个测试
```

重新生成样例（会覆盖 `sample/` 下的 Parquet 和清单）：

```bash
python3 tools/make_sample.py
```

## 目录

```
app.py                     Flask 路由、上传处理、票号解析
checker/manifest.py        清单解析与校验
checker/engine.py          批量行组跳读、逐票行号与位置删除匹配
checker/errors.py          CheckError（始终带文件名）
templates/index.html       网页
static/style.css           样式
sample/                    内置样例（清单 + Parquet）
tools/make_sample.py       样例生成脚本
tests/                     pytest 测试
Dockerfile                 容器镜像
docker-compose.yml         启动配置
```

## 已知边界

- 位置删除文件会整体读入内存建立 `(file_path, pos)` 索引；适用于常见规模。
- 数据文件按批扫描：每个需要读取的行组只读一次票号列，扫描时临时 Arrow
  列数据会被释放；内存占用主要取决于单个行组的票号列，而不是票号总数。
  逐票诊断只使用共享元数据，不会再次读取 Parquet。
- 路径按字符串精确比较，不解析 `s3://`、`file://` 等 URI 等价关系。
  请保证清单的 `original_path` 与删除记录中的 `file_path` 写法完全一致。
