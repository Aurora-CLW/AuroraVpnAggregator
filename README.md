# Aurora VPN Aggregator

> 自动聚合多源 VPN 订阅，智能测试节点有效性，生成多格式订阅链接

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Update](https://img.shields.io/badge/Update-Every%206h-orange.svg)]()

---

## ✨ 功能特性

- 🔗 **多源聚合** - 支持 GitHub 订阅、本地文件、Telegram 频道
- 🧪 **智能测试** - 自动检测节点有效性，过滤失效节点
- 📦 **多格式输出** - 支持 Clash、V2Ray、Sing-box 三种格式
- 🤖 **自动更新** - GitHub Actions 定时执行，无需人工干预
- 🌍 **地理识别** - 自动识别节点所属国家/地区
- 📊 **统计报告** - 生成节点统计信息

---

## 🚀 快速开始

### 1. 克隆项目

```bash
git clone https://github.com/YOUR_USERNAME/AuroraVpnAggregator.git
cd AuroraVpnAggregator
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置订阅源

编辑 `config/sources/github.yaml` 添加订阅源：

```yaml
type: github
enabled: true
sources:
  - name: "ermaozi_clash"
    url: "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/clash.yml"
    format: clash
    enabled: true
```

### 4. 运行

```bash
python scripts/run.py
```

### 5. 获取订阅

运行后，在 `output/` 或 `docs/` 目录获取订阅文件：

- `clash.yaml` - Clash 订阅
- `v2ray.txt` - V2Ray 订阅
- `singbox.json` - Sing-box 订阅

---

## ⚙️ 配置说明

### 订阅源配置

支持三种订阅源类型：

#### GitHub 订阅

```yaml
# config/sources/github.yaml
type: github
sources:
  - name: "订阅源名称"
    url: "https://raw.githubusercontent.com/..."
    format: clash  # clash/base64/singbox
```

#### 本地文件

```yaml
# config/sources/local.yaml
type: local
sources:
  - name: "custom"
    path: "data/sources/custom_nodes.yaml"
    format: clash
```

#### Telegram 频道

```yaml
# config/sources/telegram.yaml
type: telegram
api_id: ${TELEGRAM_API_ID}
api_hash: ${TELEGRAM_API_HASH}
channels:
  - name: "频道名称"
    channel_id: -1001234567890
    max_messages: 100
```

### 全局配置

编辑 `config/settings.yaml`：

```yaml
# 更新设置
update:
  interval: 6  # 小时

# 测试设置
testing:
  tcp:
    enabled: true
    timeout: 3
    concurrent: 100
  http:
    enabled: true
    timeout: 10
    concurrent: 20

# 输出设置
output:
  formats:
    - clash
    - v2ray
    - singbox
  max_nodes: 500
```

---

## 🤖 GitHub Actions 自动化

### 配置 Secrets

在 GitHub 仓库设置中添加 Secrets：

- `TELEGRAM_API_ID` - Telegram API ID（可选）
- `TELEGRAM_API_HASH` - Telegram API Hash（可选）

### 自动执行

项目配置了 GitHub Actions，每 6 小时自动执行：

- 抓取订阅源
- 测试节点有效性
- 生成订阅文件
- 部署到 GitHub Pages

---

## 📥 订阅地址

部署到 GitHub Pages 后，可通过以下地址访问：

```
https://YOUR_USERNAME.github.io/AuroraVpnAggregator/clash.yaml
https://YOUR_USERNAME.github.io/AuroraVpnAggregator/v2ray.txt
https://YOUR_USERNAME.github.io/AuroraVpnAggregator/singbox.json
```

---

## 📖 使用方式

### Clash 客户端

1. 打开 Clash 客户端
2. 进入「配置」→「远程配置」
3. 粘贴 Clash 订阅地址
4. 点击「更新」

### V2RayN/V2RayNG

1. 打开客户端
2. 点击「订阅」→「订阅设置」
3. 添加订阅地址
4. 更新订阅

### Sing-box

1. 打开客户端
2. 导入 Sing-box 订阅
3. 选择节点使用

---

## 📁 项目结构

```
AuroraVpnAggregator/
├── .github/workflows/     # GitHub Actions
├── config/
│   ├── settings.yaml     # 全局配置
│   └── sources/          # 订阅源配置
├── data/sources/         # 手动添加的节点
├── src/
│   ├── core/             # 核心模块
│   ├── handlers/         # 订阅源处理器
│   ├── models/           # 数据模型
│   └── utils/            # 工具函数
├── output/               # 输出文件
├── docs/                 # GitHub Pages
└── scripts/              # 入口脚本
```

---

## ⚠️ 注意事项

1. **隐私风险** - 免费节点可能存在隐私风险，请勿用于敏感操作
2. **稳定性** - 免费节点不稳定，建议多备几个订阅源
3. **合规使用** - 请遵守当地法律法规
4. **资源限制** - GitHub Actions 有每月运行时间限制

---

## 📄 License

MIT License

---

## 🙏 致谢

- [ermaozi/get_subscribe](https://github.com/ermaozi/get_subscribe)
- [mahdibland/V2RayAggregator](https://github.com/mahdibland/V2RayAggregator)
- 所有免费节点提供者
