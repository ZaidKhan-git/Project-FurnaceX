/**
 * Petroleum Intel - Dashboard JavaScript
 */

// API Base URL
const API_BASE = '';

// State
let currentFilters = {
    product: '',
    status: '',
    signal: '',
    minScore: 0
};

// ================== INITIALIZATION ==================

document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    loadLeads();
});

// ================== API CALLS ==================

async function loadStats() {
    try {
        const response = await fetch(`${API_BASE}/api/stats`);
        const stats = await response.json();
        
        document.getElementById('stat-total').textContent = stats.total_leads || 0;
        document.getElementById('stat-hot').textContent = stats.high_score_leads || 0;
        document.getElementById('stat-recent').textContent = stats.recent_leads || 0;
        
    } catch (error) {
        console.error('Failed to load stats:', error);
        showToast('Failed to load statistics', 'error');
    }
}

async function loadLeads() {
    const grid = document.getElementById('leads-grid');
    
    // Show loading
    grid.innerHTML = `
        <div class="leads-loading">
            <div class="spinner"></div>
            <p>Loading leads...</p>
        </div>
    `;
    
    try {
        // Build query params
        const params = new URLSearchParams();
        if (currentFilters.product) params.append('product', currentFilters.product);
        if (currentFilters.status) params.append('status', currentFilters.status);
        if (currentFilters.minScore > 0) params.append('min_score', currentFilters.minScore);
        params.append('limit', 50);
        
        const response = await fetch(`${API_BASE}/api/leads?${params}`);
        const data = await response.json();
        
        renderLeads(data.leads || []);
        document.getElementById('leads-count').textContent = `${data.count || 0} leads`;
        
    } catch (error) {
        console.error('Failed to load leads:', error);
        grid.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">⚠️</div>
                <div class="empty-state-text">Failed to load leads</div>
                <div class="empty-state-hint">${error.message}</div>
            </div>
        `;
    }
}

async function loadLeadDetail(leadId) {
    try {
        const response = await fetch(`${API_BASE}/api/leads/${leadId}`);
        const lead = await response.json();
        
        renderLeadModal(lead);
        openModal();
        
    } catch (error) {
        console.error('Failed to load lead detail:', error);
        showToast('Failed to load lead details', 'error');
    }
}

async function runScrapers() {
    showToast('Starting data refresh...', 'info');
    
    try {
        const response = await fetch(`${API_BASE}/api/scrape/all`, {
            method: 'POST'
        });
        const results = await response.json();
        
        // Count total new leads
        let totalNew = 0;
        for (const source in results) {
            if (results[source].inserted) {
                totalNew += results[source].inserted;
            }
        }
        
        showToast(`Refresh complete! ${totalNew} new leads found.`, 'success');
        
        // Reload data
        loadStats();
        loadLeads();
        
    } catch (error) {
        console.error('Scraper failed:', error);
        showToast('Data refresh failed', 'error');
    }
}

function exportLeads() {
    window.open(`${API_BASE}/api/leads?limit=10000&format=csv`, '_blank');
    showToast('Export started', 'info');
}

// ================== RENDERING ==================

function renderLeads(leads) {
    const grid = document.getElementById('leads-grid');
    
    if (!leads || leads.length === 0) {
        grid.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">🔍</div>
                <div class="empty-state-text">No leads found</div>
                <div class="empty-state-hint">Try running the scrapers to collect new data</div>
            </div>
        `;
        return;
    }
    
    // Filter by signal type if set
    if (currentFilters.signal) {
        leads = leads.filter(lead => lead.signal_type === currentFilters.signal);
    }
    
    grid.innerHTML = leads.map(lead => renderLeadCard(lead)).join('');
}

function renderLeadCard(lead) {
    const score = lead.score || 0;
    const scoreClass = getScoreClass(score);
    const isHot = score >= 80;
    
    // Format date
    const date = lead.discovered_at ? formatDate(lead.discovered_at) : 'Unknown';
    
    return `
        <div class="lead-card ${isHot ? 'hot' : ''}" onclick="loadLeadDetail(${lead.id})">
            <div class="lead-header">
                <div class="lead-company">${escapeHtml(lead.company_name || 'Unknown')}</div>
                <div class="lead-score ${scoreClass}">${Math.round(score)}</div>
            </div>
            <div class="lead-meta">
                <span class="lead-tag signal">${lead.signal_type || 'Signal'}</span>
                ${lead.product_match ? `<span class="lead-tag product">${lead.product_match}</span>` : ''}
                ${lead.sector ? `<span class="lead-tag sector">${lead.sector}</span>` : ''}
            </div>
            <div class="lead-source">
                <span>${lead.source || 'Unknown'}</span>
                <span class="lead-date">${date}</span>
            </div>
        </div>
    `;
}

function renderLeadModal(lead) {
    const content = document.getElementById('modal-content');
    
    // Parse keywords if string
    let keywords = lead.keywords_matched || {};
    if (typeof keywords === 'string') {
        try { keywords = JSON.parse(keywords); } catch(e) { keywords = {}; }
    }
    
    // Parse raw data
    let rawData = lead.raw_data || {};
    if (typeof rawData === 'string') {
        try { rawData = JSON.parse(rawData); } catch(e) { rawData = {}; }
    }
    
    content.innerHTML = `
        <h2 class="modal-title">${escapeHtml(lead.company_name || 'Unknown Company')}</h2>
        
        <div class="modal-section">
            <div class="modal-section-title">Signal Type</div>
            <div class="modal-section-content">${lead.signal_type || 'Unknown'}</div>
        </div>
        
        <div class="modal-section">
            <div class="modal-section-title">Score</div>
            <div class="modal-section-content">
                <span class="lead-score ${getScoreClass(lead.score)}" style="display: inline-flex;">
                    ${Math.round(lead.score || 0)}
                </span>
            </div>
        </div>
        
        ${lead.product_match ? `
        <div class="modal-section">
            <div class="modal-section-title">Likely Products</div>
            <div class="modal-section-content">${lead.product_match}</div>
        </div>
        ` : ''}
        
        ${lead.sector ? `
        <div class="modal-section">
            <div class="modal-section-title">Sector</div>
            <div class="modal-section-content">${lead.sector}</div>
        </div>
        ` : ''}
        
        <div class="modal-section">
            <div class="modal-section-title">Source</div>
            <div class="modal-section-content">
                ${lead.source}
                ${lead.source_url ? `<br><a href="${lead.source_url}" target="_blank" style="color: var(--accent-primary);">View Source →</a>` : ''}
            </div>
        </div>
        
        ${Object.keys(keywords).length > 0 ? `
        <div class="modal-section">
            <div class="modal-section-title">Matched Keywords</div>
            <div class="keywords-list">
                ${renderKeywords(keywords)}
            </div>
        </div>
        ` : ''}
        
        ${Object.keys(rawData).length > 0 ? `
        <div class="modal-section">
            <div class="modal-section-title">Raw Data</div>
            <div class="modal-section-content" style="font-size: 0.85rem; color: var(--text-secondary);">
                ${Object.entries(rawData).map(([k, v]) => 
                    `<div><strong>${k}:</strong> ${escapeHtml(String(v).substring(0, 200))}</div>`
                ).join('')}
            </div>
        </div>
        ` : ''}
        
        <div class="modal-section">
            <div class="modal-section-title">Status</div>
            <div class="modal-section-content">
                <select class="filter-select" onchange="updateLeadStatus(${lead.id}, this.value)">
                    <option value="NEW" ${lead.status === 'NEW' ? 'selected' : ''}>New</option>
                    <option value="CONTACTED" ${lead.status === 'CONTACTED' ? 'selected' : ''}>Contacted</option>
                    <option value="QUALIFIED" ${lead.status === 'QUALIFIED' ? 'selected' : ''}>Qualified</option>
                    <option value="CONVERTED" ${lead.status === 'CONVERTED' ? 'selected' : ''}>Converted</option>
                    <option value="REJECTED" ${lead.status === 'REJECTED' ? 'selected' : ''}>Rejected</option>
                </select>
            </div>
        </div>
    `;
}

function renderKeywords(keywords) {
    const allKeywords = [];
    for (const category in keywords) {
        if (Array.isArray(keywords[category])) {
            allKeywords.push(...keywords[category]);
        }
    }
    return allKeywords.map(kw => `<span class="keyword-chip">${escapeHtml(kw)}</span>`).join('');
}

// ================== FILTERS ==================

function applyFilters() {
    currentFilters.product = document.getElementById('filter-product').value;
    currentFilters.status = document.getElementById('filter-status').value;
    currentFilters.signal = document.getElementById('filter-signal').value;
    currentFilters.minScore = parseInt(document.getElementById('filter-score').value) || 0;
    
    loadLeads();
}

function updateScoreLabel() {
    const score = document.getElementById('filter-score').value;
    document.getElementById('score-label').textContent = score;
}

// ================== MODAL ==================

function openModal() {
    document.getElementById('modal-overlay').classList.add('active');
    document.body.style.overflow = 'hidden';
}

function closeModal() {
    document.getElementById('modal-overlay').classList.remove('active');
    document.body.style.overflow = '';
}

// Close on Escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeModal();
});

// ================== STATUS UPDATE ==================

async function updateLeadStatus(leadId, status) {
    try {
        await fetch(`${API_BASE}/api/leads/${leadId}/status`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ status })
        });
        
        showToast('Status updated', 'success');
        
    } catch (error) {
        console.error('Failed to update status:', error);
        showToast('Failed to update status', 'error');
    }
}

// ================== TOAST NOTIFICATIONS ==================

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    
    container.appendChild(toast);
    
    // Auto remove after 3s
    setTimeout(() => {
        toast.remove();
    }, 3000);
}

// ================== UTILITIES ==================

function getScoreClass(score) {
    if (score >= 80) return 'hot';
    if (score >= 60) return 'warm';
    if (score >= 40) return 'medium';
    return 'low';
}

function formatDate(dateStr) {
    try {
        const date = new Date(dateStr);
        const now = new Date();
        const diffMs = now - date;
        const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
        
        if (diffDays === 0) return 'Today';
        if (diffDays === 1) return 'Yesterday';
        if (diffDays < 7) return `${diffDays} days ago`;
        
        return date.toLocaleDateString('en-IN', { 
            day: 'numeric', 
            month: 'short',
            year: date.getFullYear() !== now.getFullYear() ? 'numeric' : undefined
        });
    } catch {
        return dateStr;
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
