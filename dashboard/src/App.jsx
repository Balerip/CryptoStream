import { useState, useEffect, useRef } from 'react'
import { createChart, CrosshairMode } from 'lightweight-charts'

const ES_URL = 'http://localhost:9200'

const fmt = v => v?.toLocaleString('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2
})

export default function App() {
  const [symbol, setSymbol]         = useState('BTC-USD')
  const [index, setIndex]           = useState('crypto_agg_1m')
  const [timeWindow, setTimeWindow] = useState('1m')
  const [stats, setStats]           = useState(null)
  const [ticks, setTicks]           = useState([])
  const [lastUpdate, setLastUpdate] = useState('Connecting...')
  const [connected, setConnected]   = useState(false)

  const chartContainerRef = useRef(null)
  const chartRef          = useRef(null)
  const candleRef         = useRef(null)
  const prevPriceRef      = useRef(null)
  const chartInitialized  = useRef(false)

  useEffect(() => {
    const container = chartContainerRef.current
    if (!container) return

    const initChart = (width, height) => {
      if (chartInitialized.current) return
      if (width < 10 || height < 10) return

      chartInitialized.current = true

      const c = createChart(container, {
        layout: {
          background: { color: '#0d1117' },
          textColor: '#8b949e',
        },
        grid: {
          vertLines: { color: '#21262d' },
          horzLines: { color: '#21262d' },
        },
        crosshair: { mode: CrosshairMode.Normal },
        rightPriceScale: { borderColor: '#30363d' },
        timeScale: {
          borderColor: '#30363d',
          timeVisible: true,
          secondsVisible: false,
        },
        width,
        height,
      })

      const candle = c.addCandlestickSeries({
        upColor:         '#3fb950',
        downColor:       '#f85149',
        borderUpColor:   '#3fb950',
        borderDownColor: '#f85149',
        wickUpColor:     '#3fb950',
        wickDownColor:   '#f85149',
      })

      chartRef.current  = c
      candleRef.current = candle
    }

    const ro = new ResizeObserver(entries => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect
        if (!chartInitialized.current) {
          initChart(width, height)
        } else {
          chartRef.current?.applyOptions({ width, height })
        }
      }
    })

    ro.observe(container)

    return () => {
      ro.disconnect()
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current  = null
        candleRef.current = null
        chartInitialized.current = false
      }
    }
  }, [])

  const fetchData = async () => {
    try {
      const res = await fetch(`${ES_URL}/${index}/_search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          size: 200,
          sort: [{ window_start_ts: { order: 'asc' } }],
          query: {
            bool: {
              must: [
                { match: { product_id: symbol } },
                { range: { window_start_ts: { gte: 'now-2h', lte: 'now' } } }
              ]
            }
          }
        })
      })

      const data = await res.json()
      const hits = data?.hits?.hits || []
      if (!hits.length) return

      const candles = hits.map(h => {
        const src = h._source
        const t   = Math.floor(new Date(src.window_start_ts).getTime() / 1000)
        return {
          time:  t,
          open:  src.avg_price,
          high:  src.max_price,
          low:   src.min_price,
          close: src.avg_price,
        }
      }).filter(c => c.time && c.high && c.low)

      if (candles.length) candleRef.current?.setData(candles)

      const latest = hits[hits.length - 1]._source
      const price  = latest.avg_price
      const spread = latest.max_price - latest.min_price

      setStats({
        price,
        prevPrice:  prevPriceRef.current,
        high:       latest.max_price,
        low:        latest.min_price,
        volume:     latest.total_volume,
        tickCount:  latest.tick_count,
        spread,
        spreadPct:  ((spread / price) * 100).toFixed(3),
        avg:        latest.avg_price,
        min:        latest.min_price,
        max:        latest.max_price,
        ticks:      latest.tick_count,
      })

      prevPriceRef.current = price
      setTicks(hits.slice(-10).reverse())
      setLastUpdate('Updated ' + new Date().toLocaleTimeString())
      setConnected(true)

    } catch {                                               // ← no variable at all
      setLastUpdate('ES not reachable')
      setConnected(false)
    }
  }

  useEffect(() => {
    candleRef.current?.setData([])
    fetchData() // eslint-disable-line react-hooks/exhaustive-deps
  }, [symbol, index])

  useEffect(() => {
    const id = setInterval(fetchData, 3000)
    return () => clearInterval(id)
  }, [symbol, index]) // eslint-disable-line react-hooks/exhaustive-deps

  const priceUp = stats ? stats.price >= (stats.prevPrice ?? stats.price) : true

  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>

      {/* Navbar */}
      <nav style={styles.navbar}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 24 }}>
          <div style={styles.logo}>⚡ CryptoPipeline</div>
          <div style={{ display: 'flex', gap: 4 }}>
            {['BTC-USD', 'ETH-USD'].map(s => (
              <button key={s} onClick={() => setSymbol(s)}
                style={{ ...styles.tab, ...(symbol === s ? styles.tabActive : {}) }}>
                {s.replace('-', '/')}
              </button>
            ))}
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 16, fontSize: 12, color: '#8b949e' }}>
          <span>{lastUpdate}</span>
          <div style={{
            ...styles.liveBadge,
            ...(connected ? {} : { background: '#2f1a1a', borderColor: '#f8514920', color: '#f85149' })
          }}>
            <div style={{ ...styles.liveDot, background: connected ? '#3fb950' : '#f85149' }} />
            {connected ? 'LIVE' : 'OFFLINE'}
          </div>
        </div>
      </nav>

      {/* Stats bar */}
      <div style={styles.statsBar}>
        <div style={{ fontSize: 28, fontWeight: 700, letterSpacing: -1, color: priceUp ? '#3fb950' : '#f85149' }}>
          {stats ? '$' + fmt(stats.price) : '—'}
        </div>
        {[
          { label: '24h High',   value: stats ? '$' + fmt(stats.high) : '—',                         color: '#3fb950' },
          { label: '24h Low',    value: stats ? '$' + fmt(stats.low)  : '—',                         color: '#f85149' },
          { label: '24h Volume', value: stats ? fmt(stats.volume)     : '—',                         color: '#e6edf3' },
          { label: 'Spread',     value: stats ? `$${fmt(stats.spread)} (${stats.spreadPct}%)` : '—', color: '#e6edf3' },
          { label: 'Tick Count', value: stats ? stats.tickCount       : '—',                         color: '#e6edf3' },
          { label: 'Window',     value: timeWindow,                                                   color: '#58a6ff' },
        ].map(({ label, value, color }) => (
          <div key={label} style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <div style={styles.statLabel}>{label}</div>
            <div style={{ ...styles.statValue, color }}>{value}</div>
          </div>
        ))}
      </div>

      {/* Timeframe selector */}
      <div style={styles.tfBar}>
        {[['crypto_agg_1m','1m'],['crypto_agg_5m','5m'],['crypto_agg_15m','15m']].map(([idx, label]) => (
          <button key={label}
            onClick={() => { setIndex(idx); setTimeWindow(label) }}
            style={{ ...styles.tfBtn, ...(timeWindow === label ? styles.tfBtnActive : {}) }}>
            {label}
          </button>
        ))}
      </div>

      {/* Main grid */}
      <div style={styles.main}>
        <div ref={chartContainerRef} style={styles.chartPanel} />
        <div style={styles.ticksPanel}>
          <div style={styles.panelTitle}>Live Ticks</div>
          <TicksPanel ticks={ticks} />
        </div>
        <div style={styles.metricsRow}>
          {[
            { label: 'Avg Price',    value: stats ? '$' + fmt(stats.avg)          : '—' },
            { label: 'Min Price',    value: stats ? '$' + fmt(stats.min)          : '—' },
            { label: 'Max Price',    value: stats ? '$' + fmt(stats.max)          : '—' },
            { label: 'Total Ticks',  value: stats ? stats.ticks?.toLocaleString() : '—' },
            { label: 'Total Volume', value: stats ? fmt(stats.volume)             : '—' },
          ].map(({ label, value }) => (
            <div key={label} style={styles.metricCard}>
              <div style={styles.metricLabel}>{label}</div>
              <div style={styles.metricValue}>{value}</div>
              <div style={styles.metricSub}>Last window</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function TicksPanel({ ticks }) {
  if (!ticks.length) return (
    <div style={{ color: '#8b949e', fontSize: 12, padding: 8 }}>Waiting for data...</div>
  )

  const maxVol = Math.max(...ticks.map(h => h._source.tick_count || 1))
  const fmt    = v => v?.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
  const asks   = ticks.slice(0, 5).map(h => h._source)
  const bids   = ticks.slice(5).map(h => h._source)
  const spread = asks[0] ? (asks[0].max_price - (bids[0]?.min_price || asks[0].min_price)) : 0

  return (
    <>
      {asks.map((r, i) => (
        <div key={i} style={styles.obRow}>
          <div style={{ ...styles.obBar, background: '#f85149', width: `${(r.tick_count / maxVol * 100).toFixed(0)}%` }} />
          <span style={{ color: '#f85149', fontFamily: 'monospace', fontSize: 12 }}>${fmt(r.max_price)}</span>
          <span style={{ color: '#8b949e', fontSize: 12 }}>{r.tick_count}</span>
          <span style={{ color: '#8b949e', fontSize: 12 }}>{new Date(r.window_start_ts).toLocaleTimeString()}</span>
        </div>
      ))}
      <div style={{ textAlign: 'center', padding: '6px 0', fontSize: 11, color: '#8b949e', borderTop: '1px solid #21262d', borderBottom: '1px solid #21262d', margin: '4px 0' }}>
        Spread: ${fmt(spread)}
      </div>
      {bids.map((r, i) => (
        <div key={i} style={styles.obRow}>
          <div style={{ ...styles.obBar, background: '#3fb950', width: `${(r.tick_count / maxVol * 100).toFixed(0)}%` }} />
          <span style={{ color: '#3fb950', fontFamily: 'monospace', fontSize: 12 }}>${fmt(r.min_price)}</span>
          <span style={{ color: '#8b949e', fontSize: 12 }}>{r.tick_count}</span>
          <span style={{ color: '#8b949e', fontSize: 12 }}>{new Date(r.window_start_ts).toLocaleTimeString()}</span>
        </div>
      ))}
    </>
  )
}

const styles = {
  navbar:      { display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0 24px', height: 52, background: '#161b22', borderBottom: '1px solid #30363d', flexShrink: 0 },
  logo:        { fontSize: 18, fontWeight: 700, color: '#58a6ff', letterSpacing: -0.5 },
  tab:         { padding: '6px 16px', borderRadius: 6, fontSize: 13, fontWeight: 600, cursor: 'pointer', border: '1px solid transparent', background: 'transparent', color: '#8b949e' },
  tabActive:   { background: '#1f6feb22', borderColor: '#1f6feb', color: '#58a6ff' },
  liveBadge:   { display: 'flex', alignItems: 'center', gap: 6, padding: '4px 10px', background: '#1a2f1a', border: '1px solid #2ea04320', borderRadius: 20, color: '#3fb950', fontSize: 12, fontWeight: 600 },
  liveDot:     { width: 7, height: 7, borderRadius: '50%' },
  statsBar:    { display: 'flex', alignItems: 'center', gap: 32, padding: '10px 24px', background: '#161b22', borderBottom: '1px solid #30363d', flexShrink: 0 },
  statLabel:   { fontSize: 10, color: '#8b949e', textTransform: 'uppercase', letterSpacing: 0.5 },
  statValue:   { fontSize: 13, fontWeight: 600 },
  tfBar:       { display: 'flex', gap: 4, padding: '8px 16px', background: '#161b22', borderBottom: '1px solid #30363d', flexShrink: 0 },
  tfBtn:       { padding: '3px 10px', borderRadius: 4, fontSize: 11, fontWeight: 600, cursor: 'pointer', border: '1px solid transparent', background: 'transparent', color: '#8b949e' },
  tfBtnActive: { background: '#1f6feb22', borderColor: '#1f6feb50', color: '#58a6ff' },
  main:        { display: 'grid', gridTemplateColumns: '1fr 280px', gridTemplateRows: '1fr 160px', flex: 1, gap: 1, background: '#30363d', overflow: 'hidden' },
  chartPanel:  { gridColumn: 1, gridRow: 1, background: '#0d1117', width: '100%', height: '100%' },
  ticksPanel:  { gridColumn: 2, gridRow: '1 / 3', background: '#0d1117', padding: 16, overflowY: 'auto' },
  panelTitle:  { fontSize: 11, fontWeight: 600, color: '#8b949e', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 12 },
  metricsRow:  { gridColumn: 1, gridRow: 2, display: 'flex', gap: 1, background: '#30363d' },
  metricCard:  { flex: 1, background: '#0d1117', padding: 16, display: 'flex', flexDirection: 'column', justifyContent: 'space-between' },
  metricLabel: { fontSize: 10, color: '#8b949e', textTransform: 'uppercase', letterSpacing: 0.5 },
  metricValue: { fontSize: 20, fontWeight: 700, color: '#e6edf3' },
  metricSub:   { fontSize: 11, color: '#8b949e' },
  obRow:       { display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '4px 0', position: 'relative' },
  obBar:       { position: 'absolute', right: 0, top: 0, height: '100%', opacity: 0.12, borderRadius: 2 },
}