// Multilingual Pipeline Monitor with i18n support

let ws = null;
let currentLanguage = localStorage.getItem('language') || 'en';
let translations = {};

const phaseKeys = [
    'data_loading',
    'validation',
    'entity_resolution',
    'graph_construction',
    'core_scoring',
    'supplementary_metrics',
    'result_assembly',
    'post_processing',
    'analysis_modules',
    'export_and_viz'
];

// Load translations from API
async function loadTranslations(lang) {
    const overlay = document.getElementById('loadingOverlay');
    overlay.classList.add('active');

    try {
        const response = await fetch(`http://localhost:8000/api/v1/i18n/${lang}`);
        if (!response.ok) {
            throw new Error(`Failed to load translations: ${response.statusText}`);
        }
        const data = await response.json();
        translations = data.translations;
        currentLanguage = lang;
        localStorage.setItem('language', lang);

        // Update UI
        updateAllText();
        updateLanguageButtons();

        console.log(`Translations loaded: ${lang}`);
    } catch (error) {
        console.error('Error loading translations:', error);
        alert(`Failed to load translations for ${lang}: ${error.message}`);
    } finally {
        overlay.classList.remove('active');
    }
}

// Get translated text
function t(key) {
    const keys = key.split('.');
    let value = translations;

    for (const k of keys) {
        if (value && typeof value === 'object') {
            value = value[k];
        } else {
            return key; // Fallback to key if not found
        }
    }

    return value || key;
}

// Update all text elements
function updateAllText() {
    // Header
    document.getElementById('pageTitle').textContent = t('frontend.monitor.title');
    document.getElementById('headerTitle').textContent = t('frontend.monitor.title');
    document.getElementById('headerSubtitle').textContent = t('frontend.monitor.subtitle');

    // Buttons
    document.getElementById('runPipelineBtn').textContent = t('frontend.monitor.buttons.run_pipeline');
    document.getElementById('clearLogsBtn').textContent = t('frontend.monitor.buttons.clear_logs');
    document.getElementById('reconnectBtn').textContent = t('frontend.monitor.buttons.reconnect');

    // Progress card
    document.getElementById('progressTitle').textContent = t('frontend.monitor.progress.title');
    document.getElementById('labelPersons').textContent = t('frontend.monitor.progress.stats.total_persons');
    document.getElementById('labelDuration').textContent = t('frontend.monitor.progress.stats.duration');
    document.getElementById('labelPhase').textContent = t('frontend.monitor.progress.stats.current_phase');

    // Logs card
    document.getElementById('logsTitle').textContent = t('frontend.monitor.logs.title');

    // Connection status (re-render based on current state)
    const statusEl = document.getElementById('connectionStatus');
    if (statusEl.classList.contains('connected')) {
        statusEl.textContent = t('frontend.monitor.connection.connected');
    } else if (statusEl.classList.contains('disconnected')) {
        statusEl.textContent = t('frontend.monitor.connection.disconnected');
    } else {
        statusEl.textContent = t('frontend.monitor.connection.connecting');
    }

    // Re-initialize phases
    initPhases();
}

// Update language button states
function updateLanguageButtons() {
    document.querySelectorAll('.language-switcher button').forEach(btn => {
        if (btn.dataset.lang === currentLanguage) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
}

// Initialize phase list
function initPhases() {
    const phaseList = document.getElementById('phaseList');
    phaseList.textContent = '';

    phaseKeys.forEach((key, index) => {
        const phaseId = index + 1;
        const item = document.createElement('div');
        item.className = 'phase-item';
        item.id = `phase-${phaseId}`;

        const num = document.createElement('div');
        num.className = 'phase-number';
        num.textContent = phaseId;

        const info = document.createElement('div');
        info.className = 'phase-info';

        const name = document.createElement('div');
        name.className = 'phase-name';
        name.textContent = t(`pipeline.phases.${key}`);

        const status = document.createElement('div');
        status.className = 'phase-status';
        status.textContent = t('pipeline.status.waiting');

        const duration = document.createElement('div');
        duration.className = 'phase-duration';
        duration.id = `phase-${phaseId}-duration`;

        info.appendChild(name);
        info.appendChild(status);
        item.appendChild(num);
        item.appendChild(info);
        item.appendChild(duration);
        phaseList.appendChild(item);
    });
}

// Update connection status
function updateConnectionStatus(status) {
    const statusEl = document.getElementById('connectionStatus');
    statusEl.className = `connection-status ${status}`;

    if (status === 'connected') {
        statusEl.textContent = t('frontend.monitor.connection.connected');
    } else if (status === 'disconnected') {
        statusEl.textContent = t('frontend.monitor.connection.disconnected');
    } else {
        statusEl.textContent = t('frontend.monitor.connection.connecting');
    }
}

// Add log entry
function addLog(message, type = 'info') {
    const logContainer = document.getElementById('logContainer');
    const timestamp = new Date().toLocaleTimeString(currentLanguage === 'ja' ? 'ja-JP' : 'en-US');
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

// Connect to WebSocket
function connectWebSocket() {
    updateConnectionStatus('connecting');
    addLog(t('frontend.monitor.logs.connecting'));

    ws = new WebSocket('ws://localhost:8000/ws/pipeline');

    ws.onopen = () => {
        updateConnectionStatus('connected');
        addLog(t('frontend.monitor.logs.connected'), 'complete');
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        handleWebSocketMessage(data);
    };

    ws.onerror = (error) => {
        updateConnectionStatus('disconnected');
        addLog(t('frontend.monitor.logs.error').replace('{error}', error.message), 'error');
    };

    ws.onclose = () => {
        updateConnectionStatus('disconnected');
        addLog(t('frontend.monitor.logs.disconnected'), 'error');
    };
}

// Handle WebSocket messages
function handleWebSocketMessage(data) {
    const { type } = data;

    switch (type) {
        case 'connection_established':
            addLog(data.message, 'complete');
            break;

        case 'pipeline_start':
            addLog(t('pipeline.messages.start').replace('{total_phases}', data.total_phases), 'complete');
            initPhases();
            document.getElementById('progressBar').style.width = '0%';
            document.getElementById('progressBar').textContent = '0%';
            break;

        case 'phase_update':
            addLog(`Phase ${data.phase}: ${t(`pipeline.phases.${phaseKeys[data.phase - 1]}`)} - ${t('pipeline.status.running')}`);
            updatePhase(data.phase, 'running', phaseKeys[data.phase - 1]);
            updateProgress(data.progress);
            document.getElementById('currentPhase').textContent = `${data.phase}/10`;
            break;

        case 'phase_complete':
            const phaseKey = phaseKeys[data.phase - 1];
            addLog(
                t('pipeline.messages.phase_complete')
                    .replace('{phase}', data.phase)
                    .replace('{phase_name}', t(`pipeline.phases.${phaseKey}`))
                    .replace('{duration}', data.duration_ms.toFixed(0)),
                'complete'
            );
            updatePhase(data.phase, 'complete', phaseKey, data.duration_ms);
            updateProgress(data.progress);
            break;

        case 'phase_error':
            addLog(
                t('pipeline.messages.phase_error')
                    .replace('{phase}', data.phase)
                    .replace('{phase_name}', t(`pipeline.phases.${phaseKeys[data.phase - 1]}`))
                    .replace('{error}', data.error),
                'error'
            );
            updatePhase(data.phase, 'error', phaseKeys[data.phase - 1]);
            break;

        case 'pipeline_complete':
            addLog(
                t('pipeline.messages.complete')
                    .replace('{total_persons}', data.total_persons)
                    .replace('{duration}', data.duration_seconds.toFixed(2)),
                'complete'
            );
            document.getElementById('totalPersons').textContent = data.total_persons;
            document.getElementById('duration').textContent = data.duration_seconds.toFixed(2);
            updateProgress(100);
            break;

        default:
            addLog(`Unknown message type: ${type}`);
    }
}

// Update phase display
function updatePhase(phaseId, status, phaseKey, duration) {
    const phaseEl = document.getElementById(`phase-${phaseId}`);
    if (!phaseEl) return;

    phaseEl.className = `phase-item ${status}`;

    const statusEl = phaseEl.querySelector('.phase-status');
    if (status === 'running') {
        statusEl.textContent = t('pipeline.status.running');
    } else if (status === 'complete') {
        statusEl.textContent = t('pipeline.status.completed');
    } else if (status === 'error') {
        statusEl.textContent = t('pipeline.status.error');
    } else {
        statusEl.textContent = t('pipeline.status.waiting');
    }

    if (duration !== undefined) {
        const durationEl = document.getElementById(`phase-${phaseId}-duration`);
        durationEl.textContent = `${duration.toFixed(0)}ms`;
    }
}

// Update progress bar
function updateProgress(progress) {
    const progressBar = document.getElementById('progressBar');
    progressBar.style.width = `${progress}%`;
    progressBar.textContent = `${progress.toFixed(1)}%`;
}

// Run pipeline
async function runPipeline() {
    try {
        addLog(t('frontend.monitor.logs.connecting'));
        const response = await fetch('http://localhost:8000/api/v1/pipeline/run', {
            method: 'POST'
        });
        const data = await response.json();
        addLog(`✓ ${data.message}`, 'complete');
    } catch (error) {
        addLog(`✗ ${error.message}`, 'error');
    }
}

// Clear logs
function clearLogs() {
    document.getElementById('logContainer').textContent = '';
    addLog(t('frontend.monitor.logs.cleared'));
}

// Event listeners
document.getElementById('runPipelineBtn').addEventListener('click', runPipeline);
document.getElementById('clearLogsBtn').addEventListener('click', clearLogs);
document.getElementById('reconnectBtn').addEventListener('click', () => {
    if (ws) ws.close();
    connectWebSocket();
});

// Language switcher
document.querySelectorAll('.language-switcher button').forEach(btn => {
    btn.addEventListener('click', () => {
        const lang = btn.dataset.lang;
        if (lang !== currentLanguage) {
            loadTranslations(lang);
        }
    });
});

// Initialize
(async function init() {
    await loadTranslations(currentLanguage);
    connectWebSocket();
})();
