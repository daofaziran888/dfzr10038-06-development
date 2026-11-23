# 特征工坊 · 特征工程与标注数据管理平台

面向特征注册与检索、衍生特征工程、时间窗口计算、特征回溯填充、样本分层抽取、标注任务派发、标注质量审核、特征血缘追踪与在线/离线特征服务的数据特征工程与标注管理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/window_engine.py  滑动窗口事件时间计算接口（/api/window-engine）
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/window_engine.py  事件时间计算引擎（落窗/幂等接入/补算结算/对账）
│   ├── app/services/base.py     共享服务骨架
│   ├── tests/                引擎单元测试（python -m unittest discover -s tests）
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 特征注册 | `feature_reg` | 特征定义 | 特征编号、特征名称、数据类型 |
| 衍生工程 | `derive_rule` | 衍生规则 | 规则编号、输入特征、衍生函数 |
| 时间窗口 | `time_window` | 窗口模板 | 模板编号、窗口类型、窗口宽度 |
| 特征回溯 | `backfill_job` | 回溯任务 | 任务编号、目标特征、起始日期 |
| 样本分层 | `sample_strata` | 分层策略 | 策略编号、特征范围、分层字段 |
| 标注派发 | `label_task` | 标注任务 | 任务编号、样本范围、标注类型 |
| 标注审核 | `label_review` | 审核批次 | 批次编号、标注任务、审核人 |
| 一致性计算 | `consent_calc` | 一致性报告 | 报告编号、标注任务、评估指标 |
| 特征存储 | `feature_store` | 存储配置 | 配置编号、特征列表、存储引擎 |
| 在线服务 | `online_serv` | 服务端点 | 端点编号、特征列表、QPS上限 |
| 离线导出 | `offline_exp` | 导出管道 | 管道编号、特征范围、导出格式 |
| 特征血缘 | `feature_line` | 血缘链路 | 链路编号、上游特征、下游特征 |
| 特征监控 | `feature_mon` | 监控规则 | 规则编号、目标特征、监控类型 |
| 版本管理 | `version_mgr` | 特征版本 | 版本编号、关联特征、版本标签 |
| 数据切分 | `data_split` | 切分配置 | 配置编号、数据集、切分方式 |
| 特征目录 | `feat_catalog` | 目录索引 | 索引编号、目录名称、父目录 |
| 质量规则 | `quality_rule` | 质量检查 | 规则编号、目标特征、检查类型 |
| 标注词典 | `label_dict` | 标注项 | 词典编号、标签名称、标签值 |
| 批量接入 | `batch_load` | 接入任务 | 任务编号、数据源、目标特征 |
| 特征报告 | `report_task` | 分析报表 | 报告编号、报告类型、特征范围 |
| 调度编排 | `schedule_job` | 调度计划 | 计划编号、任务类型、关联对象 |
| 治理审计 | `govern_audit` | 审计记录 | 记录编号、审计对象、审计类型 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 滑动窗口事件时间计算

`app/services/window_engine.py` 把窗口模板的宽度/滑动步长/对齐基准/时区落成
真实计算规则，接口挂在 `/api/window-engine`（先于 `/{module}` 动态路由注册）：

- 事件时间计算：带标识的历史事件按左闭右开 `[start, start+width)` 落入
  滑动窗口，写出可回溯的覆盖分区（`POST /templates/{id}/compute`，
  `GET /templates/{id}/partitions`、`GET /events/{id}/attribution` 回查归属）。
- 幂等接入：同一事件并发重放只累计一次接入量，凭据唯一
  （`POST /events`、`POST /events/{id}/replay`）。
- 补算结算：按覆盖分区实际计算窗口特征值并交付，批次记录已交付金额、
  关联分区与尚待另一窗提供的重叠范围（`POST /templates/{id}/windows/{key}/settle`）。
- 对账：各窗口结算合计与重叠事件的唯一接入凭据核对（`GET /reconcile`）。

演示场景（种子即此状态）：宽 4h / 步长 2h / 00:00 对齐 / Asia/Shanghai。
后窗 `[02:00,06:00)` 已补算 10 元（05:00 事件 3 元 + 重叠区迟到的 E7 7 元），
前窗 `[00:00,04:00)` 待执行（BACK-0005，启动回溯即重传 E7 并结算 7 元）；
04:00 事件因右开边界不进前窗，与 E7 分别计量。
运行测试：`cd backend && .venv/bin/python -m unittest discover -s tests`。

领域：数据工程
子方向：数仓与数据湖
