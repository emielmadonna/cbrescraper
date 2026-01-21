"use client";

import { useState, useEffect, useRef } from "react";

export default function Home() {
  const [url, setUrl] = useState("");
  const [isScraping, setIsScraping] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [headless, setHeadless] = useState(false);
  const [mode, setMode] = useState<"auto" | "person" | "property">("auto");

  // Test Mode State
  const [isTestMode, setIsTestMode] = useState(false);
  const [testLimit, setTestLimit] = useState<number>(3);

  // Voice Agent Query Tester
  const [query, setQuery] = useState("");
  const [queryResult, setQueryResult] = useState("");
  const [isQuerying, setIsQuerying] = useState(false);

  const logsEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // WebSocket for logs
    const wsUrl = process.env.NEXT_PUBLIC_WS_URL || "ws://localhost:8000/ws/logs";
    const ws = new WebSocket(wsUrl);
    ws.onmessage = (event) => {
      const message = event.data;
      setLogs((prev) => [...prev, message]);

      // Auto-detect finish
      if (message.includes("Scraper finished") || message.includes("Error running scraper")) {
        setIsScraping(false);
      }
    };
    return () => ws.close();
  }, []);

  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs]);

  const startScrape = async () => {
    setIsScraping(true);
    setLogs([]);
    try {
      // Determine endpoint based on mode
      let endpoint = "/api/start-scrape";
      if (mode === "person") endpoint = "/api/scrape/person";
      if (mode === "property") endpoint = "/api/scrape/property";

      const baseUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
      await fetch(`${baseUrl}${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          url,
          headless,
          dry_run: isTestMode,
          limit: isTestMode ? testLimit : null
        }),
      });
    } catch (error) {
      console.error("Failed to start scrape:", error);
      setIsScraping(false);
      alert("Failed to start scraper");
    }
  };

  const stopScrape = async () => {
    try {
      const baseUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
      await fetch(`${baseUrl}/api/stop-scrape`, { method: "POST" });
      setIsScraping(false);
    } catch (error) {
      console.error("Failed to stop scrape:", error);
    }
  };

  // API Docs State
  const [expandedEndpoint, setExpandedEndpoint] = useState<string | null>(null);

  const apiEndpoints = [
    {
      method: "POST",
      path: "/api/query/people",
      label: "People Only",
      color: "blue",
      description: "Search exclusively for brokers and professionals.",
      body: {
        query: "Find a broker in Seattle",
        top_k: 3
      }
    },
    {
      method: "POST",
      path: "/api/query/properties",
      label: "Properties Only",
      color: "purple",
      description: "Search exclusively for properties and listings.",
      body: {
        query: "Industrial warehouse in Texas",
        top_k: 3
      }
    },
    {
      method: "POST",
      path: "/api/query-voice",
      label: "Generic",
      color: "green",
      description: "Search everything (best for general intent).",
      body: {
        query: "Tell me about Joe Riley",
        top_k: 3
      }
    }
  ];

  const runVoiceQuery = async () => {
    setIsQuerying(true);
    setQueryResult("");
    try {
      const baseUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
      const res = await fetch(`${baseUrl}/api/query-voice`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
      });
      const data = await res.json();
      setQueryResult(data.text);
    } catch (err) {
      setQueryResult("Error querying voice agent.");
    } finally {
      setIsQuerying(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 p-8 font-sans">
      <div className="max-w-6xl mx-auto grid grid-cols-1 lg:grid-cols-2 gap-8">

        {/* LEFT COLUMN - CONTROLS */}
        <div className="space-y-8">

          {/* Header */}
          <div>
            <h1 className="text-3xl font-bold text-green-400 mb-2">CBRE Scraper & Voice Agent</h1>
            <p className="text-gray-400">Enterprise AI Data Extraction & Vectorization</p>
          </div>

          {/* Scraper Configuration Card */}
          <div className="bg-gray-800 p-6 rounded-xl border border-gray-700 shadow-lg">
            <h2 className="text-xl font-semibold mb-4 text-white flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-green-500"></span>
              Scraper Configuration
            </h2>

            <div className="space-y-4">
              {/* URL Input */}
              <div>
                <label className="block text-sm font-medium text-gray-400 mb-1">Target URL</label>
                <input
                  type="text"
                  value={url}
                  onChange={(e) => setUrl(e.target.value)}
                  placeholder="https://www.cbre.com/people/..."
                  className="w-full bg-gray-900 border border-gray-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-green-500 outline-none transition-all"
                />
              </div>

              {/* Mode Selection */}
              <div>
                <label className="block text-sm font-medium text-gray-400 mb-1">Scrape Mode</label>
                <div className="flex bg-gray-900 rounded-lg p-1 border border-gray-600">
                  {['auto', 'person', 'property'].map((m) => (
                    <button
                      key={m}
                      onClick={() => setMode(m as any)}
                      className={`flex-1 py-1.5 text-sm rounded-md capitalize transition-colors ${mode === m ? 'bg-green-600 text-white shadow-sm' : 'text-gray-400 hover:text-white'
                        }`}
                    >
                      {m}
                    </button>
                  ))}
                </div>
              </div>

              {/* Options Grid */}
              <div className="grid grid-cols-2 gap-4">
                <div className="flex items-center gap-3 bg-gray-900 p-3 rounded-lg border border-gray-600">
                  <input
                    type="checkbox"
                    checked={headless}
                    onChange={(e) => setHeadless(e.target.checked)}
                    className="w-5 h-5 text-green-500 rounded focus:ring-green-500 bg-gray-800 border-gray-600"
                  />
                  <span className="text-sm">Headless Mode</span>
                </div>

                <div className="flex items-center gap-3 bg-gray-900 p-3 rounded-lg border border-gray-700">
                  <input
                    type="checkbox"
                    checked={isTestMode}
                    onChange={(e) => setIsTestMode(e.target.checked)}
                    className="w-5 h-5 text-yellow-500 rounded focus:ring-yellow-500 bg-gray-800 border-gray-600"
                  />
                  <div>
                    <span className="text-sm block text-yellow-500 font-medium">Test / Dry Run</span>
                    <span className="text-xs text-gray-500">No Save to DB</span>
                  </div>
                </div>
              </div>

              {/* Test Limit Input */}
              {isTestMode && (
                <div className="bg-yellow-900/20 border border-yellow-700/50 p-4 rounded-lg">
                  <label className="block text-sm font-medium text-yellow-500 mb-1">Limit Items (Test Mode)</label>
                  <input
                    type="number"
                    value={testLimit}
                    onChange={(e) => setTestLimit(parseInt(e.target.value) || 1)}
                    className="w-full bg-gray-900 border border-yellow-700 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-yellow-500 outline-none"
                    min="1"
                  />
                  <p className="text-xs text-yellow-600 mt-1">Processing will stop after {testLimit} items.</p>
                </div>
              )}

              {/* Action Buttons */}
              <div className="pt-2 flex gap-4">
                <button
                  onClick={startScrape}
                  disabled={isScraping || !url}
                  className={`flex-1 py-3 px-6 rounded-lg font-medium transition-all transform active:scale-95 ${isScraping || !url
                    ? "bg-gray-700 text-gray-500 cursor-not-allowed"
                    : "bg-green-600 hover:bg-green-500 text-white shadow-lg hover:shadow-green-500/20"
                    }`}
                >
                  {isScraping ? "Scraping..." : "Start Scraper"}
                </button>

                {isScraping && (
                  <button
                    onClick={stopScrape}
                    className="py-3 px-6 rounded-lg font-medium bg-red-600 hover:bg-red-500 text-white shadow-lg hover:shadow-red-500/20 transition-all active:scale-95"
                  >
                    Stop
                  </button>
                )}
              </div>
            </div>
          </div>

          {/* Voice Agent Testing Playground */}
          <div className="bg-gray-800 p-6 rounded-xl border border-gray-700 shadow-lg">
            <h2 className="text-xl font-semibold mb-4 text-white flex items-center gap-2">
              <span className="text-2xl">🎙️</span>
              Voice Agent Query Tester
            </h2>
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-400 mb-1">Ask a question</label>
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Find me a broker in Seattle..."
                    className="flex-1 bg-gray-900 border border-gray-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-blue-500 outline-none"
                    onKeyDown={(e) => e.key === 'Enter' && runVoiceQuery()}
                  />
                  <button
                    onClick={runVoiceQuery}
                    disabled={isQuerying}
                    className="bg-blue-600 hover:bg-blue-500 px-4 py-2 rounded-lg font-medium transition-colors"
                  >
                    {isQuerying ? "..." : "Ask"}
                  </button>
                </div>
              </div>

              {queryResult && (
                <div className="bg-gray-900 p-4 rounded-lg border border-gray-700 animate-in fade-in slide-in-from-top-2">
                  <p className="text-xs text-gray-500 mb-1 uppercase tracking-wider">Agent Response (TTS Ready)</p>
                  <p className="text-lg text-blue-200 leading-relaxed">"{queryResult}"</p>
                </div>
              )}
            </div>
          </div>

          {/* API Endpoint Information Card */}
          <div className="bg-gray-800/50 p-6 rounded-xl border border-gray-700/50">
            <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-3">API Integration Endpoints</h3>
            <div className="space-y-2">
              {apiEndpoints.map((ep, i) => (
                <div
                  key={i}
                  className={`bg-gray-900 rounded border transition-all cursor-pointer overflow-hidden ${expandedEndpoint === ep.path ? 'border-gray-500 ring-1 ring-gray-600' : 'border-gray-700 hover:border-gray-600'
                    }`}
                  onClick={() => setExpandedEndpoint(expandedEndpoint === ep.path ? null : ep.path)}
                >
                  {/* Header Row */}
                  <div className="flex justify-between items-center p-3">
                    <div className="flex items-center gap-3">
                      <span className="font-mono text-sm text-gray-300">{ep.path}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className={`text-[10px] uppercase font-bold bg-${ep.color}-900/30 text-${ep.color}-400 px-2 py-0.5 rounded`}>
                        {ep.label}
                      </span>
                      <span className="text-gray-500 transform transition-transform duration-200">
                        {expandedEndpoint === ep.path ? '▼' : '▶'}
                      </span>
                    </div>
                  </div>

                  {/* Expanded Details */}
                  {expandedEndpoint === ep.path && (
                    <div className="bg-black/50 p-4 border-t border-gray-800 space-y-3 animate-in fade-in">
                      <p className="text-xs text-gray-400">{ep.description}</p>

                      <div>
                        <div className="text-[10px] uppercase text-gray-500 font-semibold mb-1">Request Body (JSON)</div>
                        <pre className="bg-gray-950 p-3 rounded border border-gray-800 text-xs font-mono text-blue-300 overflow-x-auto">
                          {JSON.stringify(ep.body, null, 2)}
                        </pre>
                      </div>

                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <div className="text-[10px] uppercase text-gray-500 font-semibold mb-1">Method</div>
                          <div className="text-xs font-mono text-gray-300">{ep.method}</div>
                        </div>
                        <div>
                          <div className="text-[10px] uppercase text-gray-500 font-semibold mb-1">Content-Type</div>
                          <div className="text-xs font-mono text-gray-300">application/json</div>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              ))}
              <p className="text-xs text-gray-500 pt-2 text-center">Click rows to view integration details</p>
            </div>
          </div>

        </div>

        {/* RIGHT COLUMN - LOGS */}
        <div className="flex flex-col h-[calc(100vh-4rem)]">
          <div className="bg-black rounded-t-xl border border-gray-800 p-4 flex justify-between items-center">
            <h3 className="font-mono text-sm text-gray-400">System Logs</h3>
            <span className={`w-3 h-3 rounded-full ${isScraping ? 'bg-green-500 animate-pulse' : 'bg-gray-600'}`}></span>
          </div>
          <div className="flex-1 bg-black border-x border-b border-gray-800 rounded-b-xl p-4 overflow-y-auto font-mono text-xs text-gray-300 shadow-inner">
            {logs.length === 0 ? (
              <div className="h-full flex items-center justify-center text-gray-600 italic">
                Ready to scrape...
              </div>
            ) : (
              logs.map((log, i) => (
                <div key={i} className="mb-1 break-words hover:bg-gray-900/50 px-1 rounded">
                  <span className="text-gray-600 mr-2">[{new Date().toLocaleTimeString()}]</span>
                  {log}
                </div>
              ))
            )}
            <div ref={logsEndRef} />
          </div>
        </div>

      </div>
    </div>
  );
}
