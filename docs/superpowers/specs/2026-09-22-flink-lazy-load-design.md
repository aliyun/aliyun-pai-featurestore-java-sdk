# Flink Sink 元数据按需加载改造设计

日期：2026-09-22
分支：`features/flink_lazy_load`（基于 `features/flink_sink`）
参考：master 分支已有的按需加载实现（不移植 reload worker / close() 生命周期）

## 背景

当前 `features/flink_sink` 分支上，`FeatureStoreClient` 构造函数会同步执行
`loadProjectData()`，一次性全量拉取：

- 所有 Project 及其 datasource
- 每个 project 的**全部** FeatureEntity（分页）
- 每个 project 的**全部** FeatureView（先 list 再逐个 `getFeatureViewById`，N+1 请求）
- 每个 project 的**全部** Model（同样 list + 逐个 `getModelById`）

Flink sink 在首条数据 `invoke()` 时 lazy init 触发该全量加载。问题：

1. 首条记录处理延迟 = 全量元数据拉取耗时（project 下 view/model 多时可达数秒以上）
2. sink 并行度 N = N 次全量 API 拉取，对 FeatureStore 服务端 N 倍压力
3. `getFeatureViewById` N+1 模式是主要耗时来源

master 分支已实现按需加载：`loadProjectData()` 只拉 project 列表；
`Project.getFeatureView/getModel/getFeatureEntity` 在 map miss 时按名字调 API
加载并缓存。本设计将该模式移植到当前分支，并针对 Flink 多线程场景做并发加固。

## 目标

- sink/source 初始化只加载实际用到的单个 FeatureView（+ 必要的 FeatureEntity）
- 已加载对象缓存在内存 map 中，重复访问不再调 API
- 并发触发懒加载时不重复请求、不出现 HashMap 脏读

## 非目标

- 不移植 master 的 60s 周期 reload worker
- 不移植 master 的 `FeatureStoreClient.close()` 生命周期
- 不改动 Flink sink/source 的调用方式（它们调 `project.getFeatureView(name)` 自动走懒加载）

## 改动明细（4 个文件）

### 1. `FeatureStoreClient.java`

- `loadProjectData()` 瘦身：保留 `ListProjects` + online/offline datasource +
  `createSignature` + FeatureDB datasource 注册；删除 FeatureEntity / FeatureView /
  Model 三段全量分页循环。
- 为每个 `domainProject` 注入 `setApiClient(this.apiClient)`（懒加载需要）。
- `projects` map 改为 `ConcurrentHashMap`。

### 2. `domain/Project.java`（核心）

- `featureViewMap` / `featureEntityMap` / `modelMap` 从 `HashMap` 改为 `ConcurrentHashMap`。
- 新增 `apiClient` 字段与 `setApiClient()`。
- `getFeatureView(name)`：map miss → `loadFeatureView(name)`，使用
  `FeatureViewApi.listFeatureViewsByName(name, projectId, pageNumber, pageSize)`
  按名字精确查询；命中后 `getFeatureViewById` + registerDatasource（如需要）+
  `FeatureViewFactory.getFeatureView(...)` 构建 domain 对象，放入 `featureViewMap`。
- `getSeqFeatureView(name)`：同上加载路径，返回 `SequenceFeatureView` 类型实例。
- `getFeatureEntity(name)`：miss → `loadFeatureEntities()`（entity 数量少，
  按 master 方式分页全量加载一次），从 map 返回。
- `getModel(name)` / `getModelFeature(name)`：miss → `loadModelFeature(name)`，
  使用 `FsModelApi.listModelsByName`。
- 加载异常：logger.error 后返回 null（与 master 行为一致，不向上抛）。
- **并发加固**（比 master 更严格）：
  - `loadFeatureView(name)` / `loadModelFeature(name)` 对同名 key 的加载路径加锁
    （double-checked：进锁后先查 map，已有则直接返回），避免 Flink async lookup
    多线程并发时对同一 view 重复调 API。
  - `loadFeatureEntities()` 同样加锁 + double-check，避免重复全量拉取。
  - master 用普通 HashMap 且无锁，在并发下存在脏读/重复请求隐患，本实现修正。

### 3. `api/FsModelApi.java`

- 补充 `listModelsByName(String modelFeatureName, String projectId, int pageNumber,
  int pageSize)`，从 master 移植（当前分支缺失；
  `FeatureViewApi.listFeatureViewsByName` 当前分支已存在，无需改动）。

### 4. `domain/Model.java`

- 构造函数中 `project.getFeatureViewMap().get(feature.getFeatureViewName())` 改为
  `project.getFeatureView(name)`，null 时 fallback `project.getSeqFeatureView(name)`
  （与 master 一致）。
- 仅改这一处取值逻辑，不动本分支 Model 的其他代码（如 static executorService）。

## 数据流（改造后）

```
Flink sink invoke(首条)
  → initializeFeatureView() [lazy init, 一次]
    → new FeatureStoreClient(...)
      → loadProjectData(): 仅 ListProjects + datasource（轻量）
    → project.getFeatureView(featureViewName)
      → map miss → loadFeatureView(name)  [加锁]
        → listFeatureViewsByName(name)     # 1 次按名查询
        → getFeatureViewById(id)           # 1 次
        → getFeatureEntity(entityName)     # miss 时 loadFeatureEntities()
      → 放入 featureViewMap（内存缓存）
  → 后续 invoke 直接命中缓存，零 API 调用
```

请求数从 O(全部 view + entity + model) 降为 O(1 个 view + entity 列表)。

## 错误处理

- project 不存在：`initializeFeatureView` 中已有 RuntimeException，行为不变。
- featureView 按名查询无结果：`getFeatureView` 返回 null，
  `getFeatureView` 与 `getSeqFeatureView` 均为 null 时 sink 抛
  "featureview not found"（现有逻辑，不变）。
- API 调用异常：Project 内 logger.error，返回 null，由上层现有 null 检查处理。

## 测试

1. `mvn compile`（默认 profile + `flink-1.20` profile）编译通过。
2. 现有单测不回归（依赖真实凭证/网络的测试如本地不可运行则说明）。
3. 手工验证点：
   - sink 初始化日志显示只请求了目标 featureView，不再有全量 list 循环。
   - 同名 featureView 并发 get 只触发一次 API 加载。
