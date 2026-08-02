/**
 * Data Catalog Graph — Reusable D3.js Force-Directed Graph Component
 * Usage: new DataCatalogGraph('#container', { dataUrl: '/api/catalog/graph' })
 *    OR: new DataCatalogGraph('#container', { data: catalogJSON })
 */
class DataCatalogGraph {
    constructor(selector, options = {}) {
        this.container = document.querySelector(selector);
        if (!this.container) return console.error('DataCatalogGraph: container not found');

        this.options = Object.assign({
            dataUrl: null,
            data: null,
            width: this.container.clientWidth,
            height: this.container.clientHeight || 600,
            nodeRadius: { geo_level: 18, data_source: 14, fact_table: 12, dimension: 10, domein: 11, attribuut: 6, staging: 9 },
            colors: {
                geo_level: '#5BA4B5',
                data_source: '#C9A96E',
                fact_table: '#4ade80',
                dimension: '#a78bfa',
                domein: '#f472b6',
                attribuut: '#94a3b8',
                staging: '#fbbf24'
            },
            linkColor: 'rgba(91,164,181,0.15)',
            chargeStrength: -120,
            linkDistance: 60
        }, options);

        this.nodes = [];
        this.links = [];
        this.simulation = null;
        this.selectedNode = null;
        this.filterDomain = null;
        this.searchTerm = '';

        this._init();
    }

    async _init() {
        this._buildUI();
        const data = this.options.data || await this._fetchData();
        if (!data) return;
        this.nodes = data.nodes;
        this.links = data.links;
        this._buildGraph();
        this._updateStats();
    }

    async _fetchData() {
        try {
            const url = this.options.dataUrl || 'catalog.json';
            const r = await fetch(url);
            return await r.json();
        } catch (e) {
            console.error('DataCatalogGraph: failed to load data', e);
            return null;
        }
    }

    _buildUI() {
        this.container.classList.add('dc-graph-container');
        this.container.innerHTML = `
            <div class="dc-graph-controls">
                <input type="text" placeholder="Zoek indicator..." class="dc-search">
                <select class="dc-filter">
                    <option value="">Alle domeinen</option>
                </select>
            </div>
            <div class="dc-graph-detail">
                <button class="close-btn">&times;</button>
                <h3 class="detail-name"></h3>
                <div class="type-badge"></div>
                <p class="detail-desc"></p>
                <div class="detail-meta"></div>
                <div class="relations">
                    <h4>Relaties</h4>
                    <div class="rel-list"></div>
                </div>
            </div>
            <svg></svg>
            <div class="dc-graph-legend"></div>
            <div class="dc-graph-stats"></div>
        `;

        this.svg = d3.select(this.container).select('svg');
        this.searchInput = this.container.querySelector('.dc-search');
        this.filterSelect = this.container.querySelector('.dc-filter');
        this.detailPanel = this.container.querySelector('.dc-graph-detail');

        this.searchInput.addEventListener('input', () => {
            this.searchTerm = this.searchInput.value.toLowerCase();
            this._updateVisibility();
        });
        this.filterSelect.addEventListener('change', () => {
            this.filterDomain = this.filterSelect.value || null;
            this._updateVisibility();
        });
        this.container.querySelector('.close-btn').addEventListener('click', () => {
            this.detailPanel.classList.remove('visible');
            this.selectedNode = null;
            this._clearHighlight();
        });

        // Build legend
        const legend = this.container.querySelector('.dc-graph-legend');
        const types = [
            ['geo_level', 'Gebiedsniveau'], ['data_source', 'Databron'], ['fact_table', 'Feit-tabel'],
            ['dimension', 'Dimensie'], ['domein', 'Domein'], ['attribuut', 'Indicator']
        ];
        types.forEach(([type, label]) => {
            legend.innerHTML += `<div class="legend-item"><div class="legend-dot" style="background:${this.options.colors[type]}"></div>${label}</div>`;
        });
    }

    _buildGraph() {
        const { width, height } = this.options;
        const g = this.svg.append('g');

        // Zoom
        const zoom = d3.zoom().scaleExtent([0.2, 4]).on('zoom', (e) => g.attr('transform', e.transform));
        this.svg.call(zoom);

        // Populate domain filter
        const domains = [...new Set(this.nodes.filter(n => n.type === 'domein').map(n => n.naam))].sort();
        domains.forEach(d => {
            const opt = document.createElement('option');
            opt.value = d; opt.textContent = d;
            this.filterSelect.appendChild(opt);
        });

        // Links
        this.linkElements = g.append('g').selectAll('line')
            .data(this.links).enter().append('line')
            .attr('class', 'link')
            .attr('stroke', d => d.type === 'bevat' ? this.options.linkColor : 'rgba(201,169,110,0.12)')
            .attr('stroke-width', d => d.type === 'bevat' ? 1.5 : 0.8)
            .attr('stroke-dasharray', d => d.type === 'koppelt_aan' ? '3,3' : null);

        // Nodes
        this.nodeElements = g.append('g').selectAll('g')
            .data(this.nodes).enter().append('g')
            .attr('class', 'node')
            .call(d3.drag()
                .on('start', (e, d) => { if (!e.active) this.simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
                .on('drag', (e, d) => { d.fx = e.x; d.fy = e.y; })
                .on('end', (e, d) => { if (!e.active) this.simulation.alphaTarget(0); d.fx = null; d.fy = null; })
            )
            .on('click', (e, d) => this._selectNode(d));

        this.nodeElements.append('circle')
            .attr('r', d => this.options.nodeRadius[d.type] || 6)
            .attr('fill', d => d.kleur || this.options.colors[d.type] || '#94a3b8')
            .attr('fill-opacity', 0.7)
            .attr('stroke', d => d.kleur || this.options.colors[d.type] || '#94a3b8')
            .attr('stroke-width', 1.5)
            .attr('stroke-opacity', 0.4);

        this.nodeElements.append('text')
            .text(d => d.label || d.naam)
            .attr('dx', d => (this.options.nodeRadius[d.type] || 6) + 4)
            .attr('dy', 3)
            .attr('font-size', d => d.type === 'attribuut' ? '8px' : d.type === 'geo_level' ? '11px' : '9px')
            .attr('font-weight', d => ['geo_level', 'data_source'].includes(d.type) ? '700' : '400')
            .attr('display', d => d.type === 'attribuut' ? 'none' : null);

        // Simulation
        this.simulation = d3.forceSimulation(this.nodes)
            .force('link', d3.forceLink(this.links).id(d => d.id).distance(d => {
                if (d.type === 'bevat' && d.source.type === 'geo_level') return 80;
                if (d.type === 'bevat') return 40;
                return this.options.linkDistance;
            }))
            .force('charge', d3.forceManyBody().strength(d => {
                if (d.type === 'geo_level') return -300;
                if (d.type === 'attribuut') return -30;
                return this.options.chargeStrength;
            }))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(d => (this.options.nodeRadius[d.type] || 6) + 2))
            .on('tick', () => {
                this.linkElements
                    .attr('x1', d => d.source.x).attr('y1', d => d.source.y)
                    .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
                this.nodeElements.attr('transform', d => `translate(${d.x},${d.y})`);
            });

        // Initial zoom to fit
        setTimeout(() => {
            this.svg.call(zoom.transform, d3.zoomIdentity.translate(width * 0.1, height * 0.1).scale(0.8));
        }, 1500);
    }

    _selectNode(d) {
        this.selectedNode = d;
        const panel = this.detailPanel;
        panel.classList.add('visible');

        panel.querySelector('.detail-name').textContent = d.naam;
        const badge = panel.querySelector('.type-badge');
        badge.textContent = d.type.replace('_', ' ');
        badge.style.background = (this.options.colors[d.type] || '#94a3b8') + '22';
        badge.style.color = this.options.colors[d.type] || '#94a3b8';

        panel.querySelector('.detail-desc').textContent = d.beschrijving || '';

        // Meta rows
        const meta = panel.querySelector('.detail-meta');
        let metaHTML = '';
        if (d.eenheid) metaHTML += `<div class="meta-row"><span class="label">Eenheid</span><span class="value">${d.eenheid}</span></div>`;
        if (d.bron) metaHTML += `<div class="meta-row"><span class="label">Bron</span><span class="value">${d.bron}</span></div>`;
        if (d.granulariteit) metaHTML += `<div class="meta-row"><span class="label">Niveau</span><span class="value">${d.granulariteit}</span></div>`;
        if (d.jaar_bereik) metaHTML += `<div class="meta-row"><span class="label">Periode</span><span class="value">${d.jaar_bereik}</span></div>`;
        if (d.rij_telling) metaHTML += `<div class="meta-row"><span class="label">Records</span><span class="value">${Number(d.rij_telling).toLocaleString('nl-NL')}</span></div>`;
        if (d.kwaliteit) metaHTML += `<div class="meta-row"><span class="label">Kwaliteit</span><span class="value">${d.kwaliteit}</span></div>`;
        if (d.domein) metaHTML += `<div class="meta-row"><span class="label">Domein</span><span class="value">${d.domein}</span></div>`;
        meta.innerHTML = metaHTML;

        // Relations
        const relList = panel.querySelector('.rel-list');
        const connected = this.links.filter(l =>
            (l.source.id || l.source) === d.id || (l.target.id || l.target) === d.id
        );
        relList.innerHTML = connected.slice(0, 15).map(l => {
            const other = (l.source.id || l.source) === d.id ? l.target : l.source;
            const otherNode = typeof other === 'object' ? other : this.nodes.find(n => n.id === other);
            return `<div class="rel-item" data-id="${otherNode?.id}">${l.type || 'relatie'} → ${otherNode?.naam || '?'}</div>`;
        }).join('');
        if (connected.length > 15) relList.innerHTML += `<div class="rel-item" style="color:#5e5a55">+${connected.length - 15} meer...</div>`;

        // Click on relation
        relList.querySelectorAll('.rel-item[data-id]').forEach(el => {
            el.addEventListener('click', () => {
                const target = this.nodes.find(n => n.id === el.dataset.id);
                if (target) this._selectNode(target);
            });
        });

        this._highlightConnected(d);
    }

    _highlightConnected(d) {
        const connectedIds = new Set([d.id]);
        this.links.forEach(l => {
            const sid = l.source.id || l.source;
            const tid = l.target.id || l.target;
            if (sid === d.id) connectedIds.add(tid);
            if (tid === d.id) connectedIds.add(sid);
        });

        this.nodeElements.select('circle')
            .attr('fill-opacity', n => connectedIds.has(n.id) ? 0.9 : 0.15)
            .attr('stroke-opacity', n => connectedIds.has(n.id) ? 0.8 : 0.1);
        this.nodeElements.select('text')
            .attr('fill-opacity', n => connectedIds.has(n.id) ? 1 : 0.2)
            .attr('display', n => connectedIds.has(n.id) ? null : (n.type === 'attribuut' ? 'none' : null));
        this.linkElements
            .attr('stroke-opacity', l => {
                const sid = l.source.id || l.source;
                const tid = l.target.id || l.target;
                return (sid === d.id || tid === d.id) ? 0.6 : 0.03;
            });
    }

    _clearHighlight() {
        this.nodeElements.select('circle').attr('fill-opacity', 0.7).attr('stroke-opacity', 0.4);
        this.nodeElements.select('text').attr('fill-opacity', 1).attr('display', d => d.type === 'attribuut' ? 'none' : null);
        this.linkElements.attr('stroke-opacity', 0.3);
    }

    _updateVisibility() {
        this.nodeElements.attr('display', d => {
            if (this.searchTerm && !d.naam.toLowerCase().includes(this.searchTerm) && !(d.key || '').toLowerCase().includes(this.searchTerm)) return 'none';
            if (this.filterDomain && d.type === 'attribuut' && d.domein !== this.filterDomain) return 'none';
            return null;
        });
    }

    _updateStats() {
        const stats = this.container.querySelector('.dc-graph-stats');
        const types = {};
        this.nodes.forEach(n => { types[n.type] = (types[n.type] || 0) + 1; });
        stats.textContent = `${this.nodes.length} nodes · ${this.links.length} relaties`;
    }
}
