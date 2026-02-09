let ws = null;
const phases = [
    { id: 1, name: "Data Loading" },
    { id: 2, name: "Validation" },
    { id: 3, name: "Entity Resolution" },
    { id: 4, name: "Graph Construction" },
    { id: 5, name: "Core Scoring" },
    { id: 6, name: "Supplementary Metrics" },
    { id: 7, name: "Result Assembly" },
    { id: 8, name: "Post-Processing" },
    { id: 9, name: "Analysis Modules" },
    { id: 10, name: "Export & Visualization" }
];

function initPhases() {
    const phaseList = document.getElementById('phaseList');
    phaseList.textContent = '';
    phases.forEach(phase => {
        const item = document.createElement('div');
        item.className = 'phase-item';
        item.id = `phase-${phase.id}`;

        const num = document.createElement('div');
        num.className = 'phase-number';
        num.textContent = phase.id;

        const info = document.createElement('div');
        info.className = 'phase-info';

        const name = document.createElement('div');
        name.className = 'phase-name';
        name.textContent = phase.name;

        const status = document.createElement('div');
        status.className = 'phase-status';
        status.textContent = '待機中';

        const duration = document.createElement('div');
        duration.className = 'phase-duration';
        duration.id = `phase-${phase.id}-duration`;

        info.appendChild(name);
        info.appendChild(status);
        item.appendChild(num);
        item.appendChild(info);
        item.appendChild(duration);
        phaseList.appendChild(item);
    });
}

function updateConnectionStatus(status) {
    const statusEl = document.getElementById('connectionStatus');
    statusEl.className = `connection-status ${status}`;
    statusEl.textContent = status === 'connected' ? '✓ 接続中' :
                           status === 'disconnected' ? '✗ 切断' :
                           '⟳ 接続中...';
}

function addLog(message, type = 'info') {
    const logContainer = document.getElementById('logContainer');
    const timestamp = new Date().toLocaleTimeString('ja-JP');
    const logClass = type === 'error' ? 'error' : type === 'complete' ? 'complete' : '';

    const entry = document.createElement('div');
    entry.className = `log-entry ${logClass}`;

    const ts = document.createElement('span');
    ts.className = 'timestamp';
    ts.textContent = `[${timestamp}] `;

    const msg = document.createElement('span');
    msg.className = 'type';
    msg.textContent = message;

    entry.appendChild(ts);
    entry.appendChild(msg);
    logContainer.appendChild(entry);
    logContainer.scrollTop = logContainer.scrollHeight;
}

function connectWebSocket() {
    updateConnectionStatus('connecting');
    addLog('WebSocketに接続中...');

    ws = new WebSocket('ws://localhost:8000/ws/pipeline');

    ws.onopen = () => {
        updateConnectionStatus('connected');
        addLog('WebSocket接続成功', 'complete');
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        handleWebSocketMessage(data);
    };

    ws.onerror = (error) => {
        updateConnectionStatus('disconnected');
        addLog(`WebSocketエラー: ${error.message}`, 'error');
    };

    ws.onclose = () => {
        updateConnectionStatus('disconnected');
        addLog('WebSocket接続が閉じられました', 'error');
    };
}

function handleWebSocketMessage(data) {
    const { type } = data;

    switch (type) {
        case 'connection_established':
            addLog(data.message, 'complete');
            break;

        case 'pipeline_start':
            addLog(`パイプライン開始 (${data.total_phases} phases)`, 'complete');
            initPhases();
            document.getElementById('progressBar').style.width = '0%';
            document.getElementById('progressBar').textContent = '0%';
            break;

        case 'phase_update':
            addLog(`Phase ${data.phase}: ${data.phase_name} - ${data.status}`);
            updatePhase(data.phase, 'running', data.phase_name);
            updateProgress(data.progress);
            document.getElementById('currentPhase').textContent = `${data.phase}/10`;
            break;

        case 'phase_complete':
            addLog(`✓ Phase ${data.phase}: ${data.phase_name} 完了 (${data.duration_ms}ms)`, 'complete');
            updatePhase(data.phase, 'complete', data.phase_name, data.duration_ms);
            updateProgress(data.progress);
            break;

        case 'phase_error':
            addLog(`✗ Phase ${data.phase}: ${data.phase_name} エラー - ${data.error}`, 'error');
            updatePhase(data.phase, 'error', data.phase_name);
            break;

        case 'pipeline_complete':
            addLog(`🎉 パイプライン完了! (${data.total_persons} persons, ${data.duration_seconds}s)`, 'complete');
            document.getElementById('totalPersons').textContent = data.total_persons;
            document.getElementById('duration').textContent = data.duration_seconds.toFixed(2);
            updateProgress(100);
            break;

        default:
            addLog(`Unknown message type: ${type}`);
    }
}

function updatePhase(phaseId, status, phaseName, duration) {
    const phaseEl = document.getElementById(`phase-${phaseId}`);
    if (!phaseEl) return;

    phaseEl.className = `phase-item ${status}`;

    const statusEl = phaseEl.querySelector('.phase-status');
    statusEl.textContent = status === 'running' ? '実行中...' :
                           status === 'complete' ? '完了' :
                           status === 'error' ? 'エラー' :
                           '待機中';

    if (duration !== undefined) {
        const durationEl = document.getElementById(`phase-${phaseId}-duration`);
        durationEl.textContent = `${duration.toFixed(0)}ms`;
    }
}

function updateProgress(progress) {
    const progressBar = document.getElementById('progressBar');
    progressBar.style.width = `${progress}%`;
    progressBar.textContent = `${progress.toFixed(1)}%`;
}

async function runPipeline() {
    try {
        addLog('パイプライン実行APIを呼び出し中...');
        const response = await fetch('http://localhost:8000/api/v1/pipeline/run', {
            method: 'POST'
        });
        const data = await response.json();
        addLog(`✓ ${data.message}`, 'complete');
    } catch (error) {
        addLog(`✗ パイプライン実行エラー: ${error.message}`, 'error');
    }
}

function clearLogs() {
    document.getElementById('logContainer').textContent = '';
    addLog('ログをクリアしました');
}

document.getElementById('runPipelineBtn').addEventListener('click', runPipeline);
document.getElementById('clearLogsBtn').addEventListener('click', clearLogs);
document.getElementById('reconnectBtn').addEventListener('click', () => {
    if (ws) ws.close();
    connectWebSocket();
});

initPhases();
connectWebSocket();
