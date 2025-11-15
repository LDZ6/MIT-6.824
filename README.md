# MIT 6.824 分布式系统课程项目

> **Massachusetts Institute of Technology - Distributed Systems (6.824)**  
> 基于 Go 语言实现的完整分布式存储系统，包含容错、分片和一致性保障

[![Go Version](https://img.shields.io/badge/Go-1.15+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

---

## 项目简介

本项目是 MIT 6.824 分布式系统课程的核心实验实现，通过复刻经典论文细节，构建了一个完整的、容错的、分片的分布式存储系统。系统在不可靠网络、服务器崩溃、客户端重启及 RPC 次数限制等严苛条件下，通过了全部测试样例。

### 核心特性

- **Raft 一致性协议**：实现完整的分布式共识算法
- **容错键值存储**：基于 Raft 的线性一致性 KV 服务
- **分片存储系统**：支持动态分片迁移和负载均衡
- **MapReduce 框架**：并行计算框架实现

---

## 项目结构

```
MIT-6.824/
├── 6.824-org/          # 原始代码框架（未实现）
│   └── src/
│       ├── raft/       # Raft 协议框架
│       ├── kvraft/     # 容错 KV 服务框架
│       ├── shardctrler/# 分片控制器框架
│       ├── shardkv/    # 分片 KV 服务框架
│       └── mr/         # MapReduce 框架
│
├── 6.824-pass/         # 完整实现版本（已通过测试）
│   └── src/
│       ├── raft/       # Raft 协议实现
│       ├── kvraft/     # 容错 KV 服务实现
│       ├── shardctrler/# 分片控制器实现
│       ├── shardkv/    # 分片 KV 服务实现
│       └── mr/         # MapReduce 实现
│
└── document/           # 课程资料和论文
    ├── Lab 1-4.pdf     # 实验指导文档
    └── required-reading-papers/
        ├── mapreduce.pdf
        ├── raft-extended.pdf
        └── Students' Guide to Raft.pdf
```

---

## 实验内容

### Lab 1: MapReduce

实现 MapReduce 并行计算框架，支持大规模数据处理。

**核心功能：**
- **Coordinator 节点**：中心协调器，负责任务划分和分发
- **Worker 节点**：执行 Map 和 Reduce 任务的工作节点
- **任务调度**：支持 Map 和 Reduce 两阶段任务分发
- **容错机制**：Worker 崩溃后任务自动重新分配
- **文件管理**：中间结果和最终结果的存储管理

**技术要点：**
- 任务超时检测和重新分配
- 原子性文件写入
- 多 Worker 并发执行

### Lab 2: Raft

实现 Raft 分布式共识算法，为上层服务提供一致性保障。

**核心功能：**
- **Leader 选举**：基于随机超时的选举机制
- **日志复制**：Leader 向 Follower 复制日志条目
- **持久化存储**：状态和日志的持久化
- **日志压缩**：快照机制减少存储空间
- **安全性保证**：确保已提交日志不会被覆盖

**技术要点：**
- 选举安全性（Election Safety）
- 日志匹配特性（Log Matching Property）
- 领导者完整性（Leader Completeness）
- 状态机安全性（State Machine Safety）

### Lab 3: Fault-tolerant Key/Value Service

基于 Raft 实现容错的键值存储服务。

**核心功能：**
- **线性一致性**：所有操作满足线性一致性语义
- **读写操作**：支持 `Get`、`Put`、`Append` 操作
- **客户端去重**：防止重复请求执行
- **快照机制**：定期生成快照压缩日志
- **会话管理**：客户端会话跟踪和超时处理

**技术要点：**
- 操作序列化到 Raft 日志
- 客户端请求去重（幂等性）
- 快照生成和应用
- 线性一致性验证

### Lab 4: Sharded Key/Value Service

实现分片的键值存储系统，支持动态分片迁移。

**核心功能：**
- **分片管理**：将数据分片到多个服务组（Service Group）
- **配置管理**：通过 Shard Controller 管理分片配置
- **分片迁移**：支持配置变更时的分片迁移
- **组内容错**：每个组内部使用 Raft 保证一致性
- **负载均衡**：根据配置动态调整分片分布

**技术要点：**
- 配置变更检测和处理
- 分片迁移协议（Pull 模式）
- 迁移过程中的请求处理
- 快照包含分片状态和配置信息

---

## 快速开始

### 环境要求

- **Go 版本**：1.15 或更高版本
- **操作系统**：Linux、macOS 或 Windows（WSL 推荐）

### 安装依赖

```bash
cd 6.824-pass/src
go mod download
```

### 运行测试

#### Lab 1: MapReduce

```bash
cd main
go run mrcoordinator.go pg-*.txt
go run mrworker.go wc.so
# 或运行测试脚本
bash test-mr.sh
```

#### Lab 2: Raft

```bash
cd raft
go test -run 2A  # 测试 Leader 选举
go test -run 2B  # 测试日志复制
go test -run 2C  # 测试持久化
go test -run 2D  # 测试快照
```

#### Lab 3: Fault-tolerant KV Service

```bash
cd kvraft
go test -run 3A  # 基础功能测试
go test -run 3B  # 容错测试
```

#### Lab 4: Sharded KV Service

```bash
cd shardkv
go test -run 4A  # 基础分片测试
go test -run 4B  # 分片迁移测试
```

### 完整测试套件

```bash
# 运行所有测试（可能需要较长时间）
cd 6.824-pass/src
./test.sh  # 如果存在测试脚本
```

---

## 技术实现细节

### Raft 协议实现

- **选举机制**：使用随机超时避免选举冲突
- **日志复制**：Leader 维护 `nextIndex` 和 `matchIndex` 跟踪复制进度
- **持久化**：`currentTerm`、`votedFor`、`log` 在状态变更时持久化
- **快照**：定期生成快照，压缩已应用的日志条目

### 容错 KV 服务

- **线性一致性**：所有操作通过 Raft 日志序列化
- **去重机制**：使用客户端 ID 和序列号防止重复执行
- **快照生成**：当日志大小超过阈值时触发快照
- **状态机应用**：从 `applyCh` 接收已提交操作并应用到状态机

### 分片 KV 服务

- **配置监听**：定期查询 Shard Controller 获取最新配置
- **分片迁移**：检测配置变更，拉取缺失的分片数据
- **迁移状态**：维护分片的迁移状态（Serving、Pulling、Pushing）
- **请求路由**：根据配置将请求路由到正确的服务组

### MapReduce 框架

- **任务分发**：Coordinator 维护任务队列，分配给空闲 Worker
- **超时检测**：任务超时后重新分配给其他 Worker
- **阶段管理**：Map 阶段完成后进入 Reduce 阶段
- **文件管理**：使用临时文件确保原子性写入

---

## 测试结果

所有实验均已通过完整测试套件：

- **Lab 1 (MapReduce)**：通过所有功能测试和容错测试
- **Lab 2 (Raft)**：通过 2A-2D 全部测试，包括性能和一致性测试
- **Lab 3 (KV Raft)**：通过线性一致性验证和容错测试
- **Lab 4 (Sharded KV)**：通过分片迁移和配置变更测试

### 性能指标

- **Raft 选举**：平均选举时间 < 1 秒
- **日志复制**：支持高吞吐量日志复制
- **分片迁移**：配置变更时平滑迁移，无数据丢失
- **MapReduce**：支持大规模文件并行处理

---

## 参考资料

### 必读论文

1. **MapReduce: Simplified Data Processing on Large Clusters** (2004)
   - Google 的 MapReduce 论文，Lab 1 的理论基础

2. **In Search of an Understandable Consensus Algorithm (Extended Version)** (2014)
   - Raft 算法详细说明，Lab 2 的核心参考

3. **Students' Guide to Raft** (2016)
   - Raft 实现指南，包含常见陷阱和最佳实践

### 课程资料

- [MIT 6.824 课程主页](https://pdos.csail.mit.edu/6.824/)
- [课程视频](https://www.youtube.com/watch?v=WtZ7pcRSkOA)
- [实验指导文档](./document/)

### 相关资源

- [Raft 可视化](https://raft.github.io/)
- [Go 并发编程最佳实践](https://golang.org/doc/effective_go#concurrency)

---

##  开发指南

### 代码组织

- **框架代码**：`6.824-org/` 包含原始框架，用于理解接口
- **实现代码**：`6.824-pass/` 包含完整实现，已通过测试
- **测试代码**：每个模块包含 `*_test.go` 文件

### 调试技巧

1. **使用日志**：在关键路径添加 `DPrintf` 调试输出
2. **单元测试**：先通过简单测试，再挑战复杂场景
3. **可视化工具**：使用 Raft 可视化工具理解算法行为
4. **死锁检测**：使用 `go-deadlock` 检测潜在死锁

### 常见问题

**Q: Raft 选举一直失败？**  
A: 检查超时设置和投票逻辑，确保满足选举条件。

**Q: 线性一致性测试失败？**  
A: 确保所有操作都通过 Raft 日志，且客户端去重正确实现。

**Q: 分片迁移时请求失败？**  
A: 检查迁移状态管理，确保迁移完成前正确处理请求。


[⬆ 返回顶部](#mit-6824-分布式系统课程项目)

</div>

