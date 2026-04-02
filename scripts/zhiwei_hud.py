import os
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

STATS_FILE = Path.home() / "zhiwei-docs" / "data" / "llm_stats.json"

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Zhiwei HUD - API Health Monitor</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=JetBrains+Mono:wght@400;700&display=swap');
        
        :root {
            --bg-color: #0A0A10;
            --card-bg: rgba(255, 255, 255, 0.03);
            --card-border: rgba(255, 255, 255, 0.08);
            --accent-glow: 0 0 40px rgba(99, 102, 241, 0.15);
            --text-primary: #FFFFFF;
            --text-secondary: #94A3B8;
            --primary: #6366F1;
        }

        body {
            background-color: var(--bg-color);
            background-image: 
                radial-gradient(circle at 15% 50%, rgba(99, 102, 241, 0.08), transparent 25%),
                radial-gradient(circle at 85% 30%, rgba(139, 92, 246, 0.08), transparent 25%);
            color: var(--text-primary);
            font-family: 'Outfit', sans-serif;
            margin: 0;
            padding: 40px 20px;
            display: flex;
            flex-direction: column;
            align-items: center;
            min-height: 100vh;
        }

        .header {
            text-align: center;
            margin-bottom: 40px;
            animation: fadeInDown 0.8s ease-out;
        }

        .header h1 {
            font-size: 3rem;
            margin: 0 0 10px 0;
            background: linear-gradient(to right, #818CF8, #C084FC);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 800;
        }

        .header p {
            color: var(--text-secondary);
            font-size: 1.1rem;
        }

        .dashboard-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: 24px;
            width: 100%;
            max-width: 1100px;
            animation: fadeInUp 0.8s ease-out 0.2s both;
        }

        .glass-card {
            background: var(--card-bg);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border: 1px solid var(--card-border);
            border-radius: 20px;
            padding: 24px;
            box-shadow: 0 4px 30px rgba(0, 0, 0, 0.1), inset 0 1px 0 rgba(255, 255, 255, 0.05);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
            position: relative;
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }

        .glass-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 20px 40px rgba(0,0,0,0.4), var(--accent-glow);
            border-color: rgba(99, 102, 241, 0.3);
        }

        .glow-point {
            position: absolute;
            width: 120px;
            height: 120px;
            background: rgba(99, 102, 241, 0.4);
            filter: blur(60px);
            border-radius: 50%;
            z-index: 0;
            top: -40px;
            right: -40px;
            transition: transform 0.5s;
        }
        
        .glass-card:hover .glow-point {
            transform: scale(1.3);
            background: rgba(139, 92, 246, 0.5);
        }

        .glass-card > * {
            position: relative;
            z-index: 1;
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 24px;
        }

        .provider-name {
            font-size: 1.3rem;
            font-weight: 600;
            text-transform: capitalize;
            color: #F8FAFC;
        }

        .status-badge {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
            letter-spacing: 0.5px;
            text-shadow: 0 0 10px currentColor;
        }
        .status-healthy { background: rgba(16, 185, 129, 0.1); color: #34D399; border: 1px solid rgba(16, 185, 129, 0.2); }
        .status-warning { background: rgba(245, 158, 11, 0.1); color: #FBBF24; border: 1px solid rgba(245, 158, 11, 0.2); }
        .status-danger { background: rgba(239, 68, 68, 0.1); color: #F87171; border: 1px solid rgba(239, 68, 68, 0.2); animation: warningPulse 2s infinite; }

        @keyframes warningPulse {
            0% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.4); }
            70% { box-shadow: 0 0 0 8px rgba(239, 68, 68, 0); }
            100% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0); }
        }

        .stat-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
            padding-bottom: 12px;
            border-bottom: 1px dashed rgba(255,255,255,0.05);
        }

        .stat-row:last-child {
            margin-bottom: 0;
            padding-bottom: 0;
            border-bottom: none;
        }

        .stat-label {
            color: var(--text-secondary);
            font-size: 0.95rem;
        }

        .metric-group {
            text-align: right;
            display: flex;
            flex-direction: column;
        }

        .stat-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.5rem;
            font-weight: 700;
        }

        .value-success { color: #34D399; }
        .value-danger { color: #F87171; }
        .value-neutral { color: #94A3B8; }
        .value-warning { color: #FBBF24; }

        .last-time {
            font-size: 0.75rem;
            color: #475569;
            font-family: 'JetBrains Mono', monospace;
            margin-top: 2px;
        }

        /* Animations */
        @keyframes fadeInDown {
            from { opacity: 0; transform: translateY(-20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        @keyframes fadeInUp {
            from { opacity: 0; transform: translateY(20px); }
            to { opacity: 1; transform: translateY(0); }
        }

        /* Summary Card */
        .summary-card {
            grid-column: 1 / -1;
            background: linear-gradient(135deg, rgba(30, 27, 75, 0.6) 0%, rgba(15, 23, 42, 0.6) 100%);
            border-color: rgba(99, 102, 241, 0.2);
            flex-direction: row;
            justify-content: space-around;
            align-items: center;
            padding: 30px;
        }

        .summary-item {
            text-align: center;
        }
        
        .summary-item .stat-label {
            font-size: 1.1rem;
            margin-bottom: 8px;
            color: #CBD5E1;
        }
        .summary-item .stat-value {
            font-size: 3rem;
            background: linear-gradient(to bottom right, #38BDF8, #818CF8);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .summary-card.tripped {
            background: linear-gradient(135deg, rgba(127, 29, 29, 0.6) 0%, rgba(15, 23, 42, 0.6) 100%);
            border-color: rgba(239, 68, 68, 0.4);
        }

        .tripped-alert {
            display: flex;
            align-items: center;
            gap: 12px;
            color: #F87171;
            font-size: 1.2rem;
            font-weight: 600;
        }

        .progress-bar-bg {
            width: 100px;
            height: 6px;
            background: rgba(255,255,255,0.1);
            border-radius: 4px;
            overflow: hidden;
            margin-top: 6px;
            display: inline-block;
        }

        .progress-bar-fill {
            height: 100%;
            background: #F87171;
            width: 0%;
            transition: width 0.3s;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>Zhiwei HUD ⚡</h1>
        <p>AI Engine Token Flow & Circuit Monitor</p>
    </div>

    <div class="dashboard-grid" id="grid">
        <div style="text-align:center; grid-column:1/-1; color:#94A3B8;">Loading telemetrics...</div>
    </div>

    <script>
        function formatTime(isoStr) {
            if (!isoStr) return 'Never';
            const date = new Date(isoStr);
            return date.toLocaleTimeString('en-US', { hour12: false }) + ' ' + date.toLocaleDateString();
        }

        async function fetchData() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                renderDashboard(data);
            } catch (err) {
                console.error("Failed fetching stats", err);
            }
        }

        function renderDashboard(data) {
            const grid = document.getElementById('grid');
            if(data.error) {
                grid.innerHTML = `<div style="text-align:center; grid-column:1/-1; color:#F87171;">Error loading data: ${data.error}</div>`;
                return;
            }

            let totalSuccess = 0;
            let totalFails = 0;
            let maxConsecutiveFails = 0;
            let cardsHtml = '';

            const keys = Object.keys(data).filter(k => typeof data[k] === 'object');
            
            for (const provider of keys) {
                const stats = data[provider];
                totalSuccess += stats.success || 0;
                totalFails += stats.fail || 0;
                const cFail = stats.consecutive_fail || 0;
                if (cFail > maxConsecutiveFails) maxConsecutiveFails = cFail;

                let statusClass = 'status-healthy';
                let statusText = 'HEALTHY';
                if (cFail > 0 && cFail < 3) {
                    statusClass = 'status-warning';
                    statusText = 'DEGRADED';
                } else if (cFail >= 3) {
                    statusClass = 'status-danger';
                    statusText = 'TRIPPED';
                }

                cardsHtml += `
                    <div class="glass-card">
                        <div class="glow-point"></div>
                        <div class="card-header">
                            <span class="provider-name">${provider.replace('_', ' ')}</span>
                            <span class="status-badge ${statusClass}">${statusText}</span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Valid Calls</span>
                            <div class="metric-group">
                                <span class="stat-value value-success">${stats.success || 0}</span>
                                <span class="last-time">${formatTime(stats.last_success)}</span>
                            </div>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Errors</span>
                            <div class="metric-group">
                                <span class="stat-value ${stats.fail ? 'value-warning' : 'value-neutral'}">${stats.fail || 0}</span>
                            </div>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label" style="color: ${cFail >= 3 ? '#F87171' : ''};">Consecutive Fails</span>
                            <div class="metric-group">
                                <span class="stat-value" style="color: ${cFail >= 3 ? '#F87171' : ''};">${cFail} / 3</span>
                                <div class="progress-bar-bg">
                                    <div class="progress-bar-fill" style="width: ${Math.min((cFail/3)*100, 100)}%; background: ${cFail >= 3 ? '#F87171' : '#FBBF24'}"></div>
                                </div>
                            </div>
                        </div>
                    </div>
                `;
            }

            const isTripped = maxConsecutiveFails >= 3;
            
            const summaryHtml = `
                <div class="glass-card summary-card ${isTripped ? 'tripped' : ''}">
                    <div class="summary-item">
                        <div class="stat-label">Total Execution Ticks</div>
                        <div class="stat-value">${totalSuccess}</div>
                    </div>
                    <div class="summary-item">
                        <div class="stat-label">Dead-loop Traps Found</div>
                        <div class="stat-value" style="background: linear-gradient(to right, #F87171, #FCA5A5); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
                            ${totalFails}
                        </div>
                    </div>
                    ${isTripped ? `
                        <div class="summary-item" style="border-left: 1px solid rgba(255,255,255,0.1); padding-left: 30px;">
                            <div class="tripped-alert">
                                🛑 CIRCUIT BREAKER ACTIVE<br>
                                AI auto-reasoning halted.
                            </div>
                        </div>
                    ` : ''}
                </div>
            `;

            grid.innerHTML = summaryHtml + cardsHtml;
        }

        fetchData();
        setInterval(fetchData, 3000); // 3 sec poll
    </script>
</body>
</html>
"""

class HUDHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/api/stats':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            data = {}
            if STATS_FILE.exists():
                try:
                    data = json.loads(STATS_FILE.read_text())
                except Exception as e:
                    data = {"error": str(e)}
            
            self.wfile.write(json.dumps(data).encode('utf-8'))
            
        elif self.path == '/' or self.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass

if __name__ == '__main__':
    port = 8899
    server = HTTPServer(('0.0.0.0', port), HUDHandler)
    print(f"🚀 Zhiwei HUD running at http://localhost:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\\nHUD stopped.")
