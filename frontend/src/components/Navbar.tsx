// filepath: frontend/src/components/Navbar.tsx
import React from 'react';
import { ShoppingCart, Search, Activity, Cpu, Server, CheckCircle2, AlertCircle } from 'lucide-react';
import { MOCK_USERS } from '../data/mockProducts';
import type { HealthStatus } from '../services/api';


interface NavbarProps {
  currentUserId: string;
  onSelectUser: (userId: string) => void;
  cartCount: number;
  cartTotal: number;
  onOpenCart: () => void;
  onOpenTelemetry: () => void;
  health: HealthStatus;
  latencyMs: number;
  searchTerm: string;
  onSearchChange: (query: string) => void;
  selectedCategory: string;
  onSelectCategory: (cat: string) => void;
}

export const Navbar: React.FC<NavbarProps> = ({
  currentUserId,
  onSelectUser,
  cartCount,
  cartTotal,
  onOpenCart,
  onOpenTelemetry,
  health,
  latencyMs,
  searchTerm,
  onSearchChange,
  selectedCategory,
  onSelectCategory,
}) => {
  const categories = ['All Departments', 'Home & Living', 'Kitchen & Dining', 'Storage & Travel', 'Lunch Boxes', 'Baking & Kitchen', 'Party & Entertaining'];

  return (
    <header className="sticky top-0 z-40 bg-[#131921] text-white shadow-md">
      {/* Top Main Navigation Bar */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16 gap-4">
          
          {/* Logo / Brand */}
          <div className="flex items-center gap-3 shrink-0">
            <div className="flex items-center gap-2 cursor-pointer">
              <div className="bg-[#ff9900] text-[#131921] font-black px-2.5 py-1 rounded tracking-tight text-lg shadow-sm">
                HYBRID RS
              </div>
              <div className="hidden sm:block">
                <span className="text-xs font-semibold uppercase tracking-wider text-amber-400 block">Lakehouse & Streaming</span>
                <span className="text-sm font-bold text-gray-200">Recommendation Platform</span>
              </div>
            </div>
          </div>

          {/* Amazon-style Search Bar */}
          <div className="flex-1 max-w-2xl hidden md:flex items-center">
            <div className="flex w-full rounded-md overflow-hidden bg-white text-gray-900 focus-within:ring-2 focus-within:ring-[#ff9900]">
              <select
                value={selectedCategory}
                onChange={(e) => onSelectCategory(e.target.value)}
                className="bg-gray-100 text-xs font-medium text-gray-700 px-3 py-2 border-r border-gray-300 outline-none hover:bg-gray-200 cursor-pointer"
              >
                {categories.map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
              <input
                type="text"
                value={searchTerm}
                onChange={(e) => onSearchChange(e.target.value)}
                placeholder="Search products by title or StockCode (e.g. 85099B, Lunch Bag)..."
                className="flex-1 px-3 py-2 text-sm outline-none text-gray-800 placeholder-gray-400"
              />
              <button className="bg-[#febd69] hover:bg-[#f3a847] px-4 py-2 transition-colors flex items-center justify-center text-gray-900 cursor-pointer">
                <Search className="w-5 h-5" />
              </button>
            </div>
          </div>

          {/* Right Action Icons: User, Telemetry & Cart */}
          <div className="flex items-center gap-4 shrink-0">
            
            {/* Live Serving Latency Pill */}
            <div
              onClick={onOpenTelemetry}
              title="Click to view distributed telemetry dashboard"
              className="hidden lg:flex items-center gap-1.5 px-2.5 py-1 rounded bg-[#232f3e] border border-gray-700 hover:border-amber-400 transition cursor-pointer"
            >
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
              </span>
              <span className="text-xs text-gray-300 font-mono">P99 Latency:</span>
              <span className="text-xs font-bold text-emerald-400 font-mono">{latencyMs}ms</span>
            </div>

            {/* User Profile Selector (NCF Personalization Switcher) */}
            <div className="flex flex-col text-left">
              <span className="text-[10px] text-gray-400">Personalized User</span>
              <select
                value={currentUserId}
                onChange={(e) => onSelectUser(e.target.value)}
                className="bg-[#232f3e] text-xs font-semibold text-white px-2 py-1 rounded border border-gray-700 hover:border-gray-500 outline-none cursor-pointer"
              >
                {MOCK_USERS.map((u) => (
                  <option key={u.id} value={u.id}>
                    {u.id === 'guest' ? '👤 Guest (Cold Start)' : `👤 #${u.id} - ${u.name.split(' ')[0]}`}
                  </option>
                ))}
              </select>
            </div>

            {/* Architecture / Telemetry Monitor Button */}
            <button
              onClick={onOpenTelemetry}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded bg-[#232f3e] hover:bg-[#2e3e52] border border-gray-700 transition text-xs font-medium text-amber-400 cursor-pointer"
              title="Open Kafka & Lakehouse Telemetry Monitor"
            >
              <Activity className="w-4 h-4 text-amber-400" />
              <span className="hidden sm:inline">Telemetry</span>
            </button>

            {/* Cart Button */}
            <button
              onClick={onOpenCart}
              className="relative flex items-center gap-2 px-3 py-1.5 rounded hover:bg-[#232f3e] transition border border-transparent hover:border-gray-700 cursor-pointer"
            >
              <div className="relative">
                <ShoppingCart className="w-6 h-6 text-white" />
                {cartCount > 0 && (
                  <span className="absolute -top-1.5 -right-2 bg-[#ff9900] text-[#131921] font-bold text-xs w-5 h-5 rounded-full flex items-center justify-center border-2 border-[#131921]">
                    {cartCount}
                  </span>
                )}
              </div>
              <div className="hidden sm:flex flex-col text-left">
                <span className="text-[10px] text-gray-400">Active Cart</span>
                <span className="text-xs font-bold text-amber-400">£{cartTotal.toFixed(2)}</span>
              </div>
            </button>

          </div>
        </div>
      </div>

      {/* Sub-header / System Status Strip (AWS Console Style) */}
      <div className="bg-[#232f3e] px-4 py-1.5 text-xs text-gray-300 border-t border-gray-700 flex items-center justify-between">
        <div className="max-w-7xl mx-auto w-full flex items-center justify-between flex-wrap gap-2">
          <div className="flex items-center gap-4 flex-wrap">
            <span className="flex items-center gap-1 text-gray-400">
              <Server className="w-3.5 h-3.5 text-blue-400" />
              <span>FastAPI Serving:</span>
              {health.status === 'online' ? (
                <span className="text-emerald-400 font-semibold flex items-center gap-1">
                  <CheckCircle2 className="w-3 h-3" /> Ready
                </span>
              ) : (
                <span className="text-amber-400 font-semibold flex items-center gap-1">
                  <AlertCircle className="w-3 h-3" /> Standalone Sim
                </span>
              )}
            </span>

            <span className="flex items-center gap-1 text-gray-400">
              <Cpu className="w-3.5 h-3.5 text-purple-400" />
              <span>Models:</span>
              <span className="text-gray-200">FP-Growth (Association Rules) + PyTorch NCF (Latent Factor)</span>
            </span>

            <span className="hidden md:flex items-center gap-1 text-gray-400">
              <span>Feature Store:</span>
              <span className="text-emerald-400 font-mono font-medium">Redis O(1) Cache (TTL 3600s)</span>
            </span>
          </div>

          <div className="text-gray-400 hidden lg:block">
            Lakehouse Partition: <span className="text-gray-200 font-mono">805,549 Events Processed</span>
          </div>
        </div>
      </div>
    </header>
  );
};
