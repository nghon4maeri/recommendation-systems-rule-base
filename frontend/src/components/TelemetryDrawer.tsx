// filepath: frontend/src/components/TelemetryDrawer.tsx
import React from 'react';
import { X, Activity, Zap, Database, Cpu, Radio, CheckCircle, Terminal } from 'lucide-react';
import type { TelemetryLog } from '../types';


interface TelemetryDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  logs: TelemetryLog[];
  latencyMs: number;
  onEmitSimulatedEvent: (type: 'view' | 'add_to_cart' | 'purchase') => void;
}

export const TelemetryDrawer: React.FC<TelemetryDrawerProps> = ({
  isOpen,
  onClose,
  logs,
  latencyMs,
  onEmitSimulatedEvent,
}) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 overflow-hidden">
      {/* Backdrop */}
      <div
        onClick={onClose}
        className="absolute inset-0 bg-black/60 backdrop-blur-xs transition-opacity duration-300"
      />

      <div className="fixed inset-y-0 right-0 max-w-full flex pl-10">
        <div className="w-screen max-w-2xl bg-[#0f172a] text-gray-200 shadow-2xl flex flex-col justify-between border-l border-slate-800">
          
          {/* Header */}
          <div className="p-4 bg-[#1e293b] text-white flex items-center justify-between border-b border-slate-700">
            <div className="flex items-center gap-2">
              <Activity className="w-5 h-5 text-emerald-400" />
              <div>
                <h2 className="text-base font-bold">AWS CloudWatch / Stream Telemetry</h2>
                <p className="text-[11px] text-gray-400">Live Architecture Telemetry & Kafka Event Ingestion Monitor</p>
              </div>
            </div>
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-white transition p-1 cursor-pointer"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Infrastructure Health Cards */}
          <div className="p-4 grid grid-cols-2 sm:grid-cols-4 gap-3 bg-[#131d2e] border-b border-slate-800">
            <div className="bg-[#1e293b] p-3 rounded-lg border border-slate-700">
              <div className="flex items-center justify-between text-xs text-gray-400">
                <span>Serving Latency</span>
                <Zap className="w-3.5 h-3.5 text-amber-400" />
              </div>
              <div className="text-xl font-bold font-mono text-emerald-400 mt-1">{latencyMs} ms</div>
              <div className="text-[10px] text-emerald-500 font-semibold mt-0.5">SLA &lt; 5ms Met</div>
            </div>

            <div className="bg-[#1e293b] p-3 rounded-lg border border-slate-700">
              <div className="flex items-center justify-between text-xs text-gray-400">
                <span>Redis Feature Store</span>
                <Database className="w-3.5 h-3.5 text-blue-400" />
              </div>
              <div className="text-xl font-bold font-mono text-blue-400 mt-1">O(1)</div>
              <div className="text-[10px] text-blue-300 mt-0.5">TTL: 3,600s</div>
            </div>

            <div className="bg-[#1e293b] p-3 rounded-lg border border-slate-700">
              <div className="flex items-center justify-between text-xs text-gray-400">
                <span>Kafka Topic</span>
                <Radio className="w-3.5 h-3.5 text-purple-400" />
              </div>
              <div className="text-xs font-bold font-mono text-purple-300 mt-2 truncate">ecom.events.raw</div>
              <div className="text-[10px] text-emerald-400 font-semibold mt-0.5">Partition 0-2 Live</div>
            </div>

            <div className="bg-[#1e293b] p-3 rounded-lg border border-slate-700">
              <div className="flex items-center justify-between text-xs text-gray-400">
                <span>PyTorch NCF</span>
                <Cpu className="w-3.5 h-3.5 text-rose-400" />
              </div>
              <div className="text-xl font-bold font-mono text-rose-300 mt-1">32-dim</div>
              <div className="text-[10px] text-gray-400 mt-0.5">Embeddings Ready</div>
            </div>
          </div>

          {/* Event Simulation Trigger Bar */}
          <div className="px-4 py-2.5 bg-[#162032] border-b border-slate-800 flex items-center justify-between flex-wrap gap-2">
            <span className="text-xs text-gray-400 flex items-center gap-1.5">
              <Terminal className="w-3.5 h-3.5 text-emerald-400" />
              Simulate Producer Event to Kafka:
            </span>
            <div className="flex items-center gap-2">
              <button
                onClick={() => onEmitSimulatedEvent('view')}
                className="px-2.5 py-1 text-xs bg-slate-800 hover:bg-slate-700 text-blue-300 rounded border border-slate-600 font-mono transition cursor-pointer"
              >
                + view event
              </button>
              <button
                onClick={() => onEmitSimulatedEvent('add_to_cart')}
                className="px-2.5 py-1 text-xs bg-slate-800 hover:bg-slate-700 text-amber-300 rounded border border-slate-600 font-mono transition cursor-pointer"
              >
                + add_to_cart
              </button>
              <button
                onClick={() => onEmitSimulatedEvent('purchase')}
                className="px-2.5 py-1 text-xs bg-slate-800 hover:bg-slate-700 text-emerald-300 rounded border border-slate-600 font-mono transition cursor-pointer"
              >
                + purchase
              </button>
            </div>
          </div>

          {/* Live Log Stream Table */}
          <div className="flex-1 overflow-y-auto p-4 space-y-2 font-mono text-xs">
            <div className="text-gray-400 text-[11px] mb-2 flex items-center justify-between">
              <span>STREAM TELEMETRY AUDIT TRAIL</span>
              <span>Showing last {logs.length} entries</span>
            </div>

            {logs.length === 0 ? (
              <div className="text-center py-12 text-gray-500">
                No telemetry events logged yet. Perform interactions on the storefront.
              </div>
            ) : (
              logs.map((log) => {
                const badgeColor =
                  log.event === 'purchase'
                    ? 'text-emerald-400 bg-emerald-950/80 border-emerald-800'
                    : log.event === 'add_to_cart'
                    ? 'text-amber-400 bg-amber-950/80 border-amber-800'
                    : log.event === 'view'
                    ? 'text-blue-400 bg-blue-950/80 border-blue-800'
                    : 'text-purple-400 bg-purple-950/80 border-purple-800';

                return (
                  <div
                    key={log.id}
                    className="p-2.5 rounded bg-[#1e293b]/70 border border-slate-800 hover:border-slate-700 transition flex flex-col gap-1"
                  >
                    <div className="flex items-center justify-between text-[11px]">
                      <div className="flex items-center gap-2">
                        <span className={`px-1.5 py-0.5 rounded border text-[10px] font-bold uppercase ${badgeColor}`}>
                          {log.event}
                        </span>
                        <span className="text-gray-400">{log.timestamp}</span>
                      </div>
                      <span className="text-emerald-400 font-bold">{log.latencyMs} ms</span>
                    </div>

                    <div className="text-gray-300 text-xs mt-0.5">
                      {log.details}
                    </div>

                    <div className="text-[10px] text-gray-500 flex items-center gap-3">
                      <span>Session: <strong className="text-gray-400">{log.sessionId}</strong></span>
                      {log.stockCode && <span>StockCode: <strong className="text-gray-400">#{log.stockCode}</strong></span>}
                      {log.userId && <span>User: <strong className="text-gray-400">#{log.userId}</strong></span>}
                    </div>
                  </div>
                );
              })
            )}
          </div>

          {/* Footer */}
          <div className="p-3 bg-[#1e293b] border-t border-slate-800 flex items-center justify-between text-xs text-gray-400">
            <div className="flex items-center gap-1.5">
              <CheckCircle className="w-3.5 h-3.5 text-emerald-400" />
              <span>Lakehouse Streaming Pipeline Healthy</span>
            </div>
            <span className="font-mono text-gray-500">Schema: Avro/JSON v1.0</span>
          </div>

        </div>
      </div>
    </div>
  );
};
