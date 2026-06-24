# X Scraper

基于 Playwright 的 X/Twitter 推文采集系统，支持定时任务、多账号管理、内容检索。

## 功能

- **多账号管理** — 分组、标签、状态筛选
- **定时/手动采集** — 按间隔或 Cron 表达式调度
- **内置管理后台** — 单 HTML 文件，Vue.js 驱动
- **结构化日志** — JSONL 文件 + SQLite 数据库双写
- **内容检索** — 全文搜索、导出 CSV

## 环境要求

- Python 3.12+
- Playwright（自动安装 Chromium）
- Windows / macOS / Linux

## 快速开始

```bash
# 1. 克隆项目
git clone https://github.com/steambreadcuiyao/X-Scraper.git
cd X-Scraper

# 2. 创建虚拟环境
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

# 3. 安装依赖
pip install -r backend/requirements.txt

# 4. 安装 Playwright 浏览器
playwright install chromium

# 5. 配置环境变量
cp .env.example .env
# 编辑 .env 修改配置

# 6. 启动服务
python -m uvicorn main:app --host 0.0.0.0 --port 8765
```

打开 http://localhost:8765 访问管理后台。

## 配置说明

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `BROWSER_MODE` | `chrome` 内置 Chromium / `chrome-visible` 系统 Chrome / `cdp` 连接已运行 Chrome | `chrome` |
| `CDP_PORT` | CDP 模式端口 | `9222` |
| `X_SCRAPER_PROXY` | HTTP 代理地址 | 空 |
| `PORT` | 服务器端口 | `8765` |

## 项目结构

```
x-scraper/
├── backend/
│   ├── main.py                # FastAPI 应用入口
│   ├── database.py            # SQLite 数据库层
│   ├── logging_config.py      # 日志系统
│   ├── requirements.txt       # Python 依赖
│   └── scraper/
│       ├── __init__.py
│       └── playwright_scraper.py  # 采集核心
├── frontend/
│   └── index.html             # 管理后台 (Vue.js 单页)
├── data/                      # 运行时数据 (SQLite + 日志)
├── .env.example               # 配置模板
└── start_backend.bat          # Windows 启动脚本
```

## License

MIT
