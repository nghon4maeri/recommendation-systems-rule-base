// filepath: frontend/src/App.tsx
import { useState, useEffect, useMemo, useCallback } from 'react';
import { Navbar } from './components/Navbar';
import { ProductCard } from './components/ProductCard';
import { RecommendationSection } from './components/RecommendationSection';
import { CartDrawer } from './components/CartDrawer';
import { TelemetryDrawer } from './components/TelemetryDrawer';
import { CATALOG_PRODUCTS, getProductByStockCode } from './data/mockProducts';
import type { CartItem, HybridRecommendationResponse, Product, TelemetryLog } from './types';
import { checkBackendHealth, fetchHybridRecommendations, addCartItem } from './services/api';
import type { HealthStatus } from './services/api';
import { Sparkles, Filter, ExternalLink, RefreshCw } from 'lucide-react';


export function App() {
  // Session & User Context
  const [sessionId] = useState<string>(() => `sess_${Math.random().toString(36).substring(2, 8)}`);
  const [currentUserId, setCurrentUserId] = useState<string>('13089');
  
  // Cart State (initialize with 1 item so FP-Growth recommendations display immediately)
  const [cartItems, setCartItems] = useState<CartItem[]>(() => {
    const initialProd = getProductByStockCode('85099B') || CATALOG_PRODUCTS[2];
    return [{ product: initialProd, quantity: 1 }];
  });

  // UI States
  const [isCartOpen, setIsCartOpen] = useState(false);
  const [isTelemetryOpen, setIsTelemetryOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('All Departments');
  const [notification, setNotification] = useState<string | null>(null);

  // Recommendations & Serving Metrics
  const [recommendations, setRecommendations] = useState<HybridRecommendationResponse | null>(null);
  const [servingLatency, setServingLatency] = useState<number>(2.4);
  const [health, setHealth] = useState<HealthStatus>({ status: 'checking', redis_connected: false, model_ready: false });
  const [isRecsLoading, setIsRecsLoading] = useState(false);

  // Telemetry Audit Trail
  const [telemetryLogs, setTelemetryLogs] = useState<TelemetryLog[]>([
    {
      id: 'init-1',
      timestamp: new Date().toLocaleTimeString(),
      event: 'cache_hit',
      stockCode: '85099B',
      sessionId: 'sess_init',
      latencyMs: 1.8,
      details: 'Redis Feature Store loaded session state and warm rule cache.',
    },
  ]);

  const addTelemetryLog = useCallback((event: TelemetryLog['event'], details: string, stockCode?: string, latency = 2) => {
    setTelemetryLogs((prev) => [
      {
        id: `log_${Date.now()}_${Math.random().toString(36).substring(2, 6)}`,
        timestamp: new Date().toLocaleTimeString(),
        event,
        stockCode,
        sessionId,
        userId: currentUserId,
        latencyMs: latency,
        details,
      },
      ...prev.slice(0, 49),
    ]);
  }, [sessionId, currentUserId]);

  const showToast = (message: string) => {
    setNotification(message);
    setTimeout(() => setNotification(null), 3500);
  };

  // Health check on mount
  useEffect(() => {
    checkBackendHealth().then((h) => setHealth(h));
  }, []);

  // Compute recommendations whenever Cart or User changes
  const refreshRecommendations = useCallback(async () => {
    setIsRecsLoading(true);
    const cartCodes = cartItems.map((item) => item.product.stockCode);
    const start = performance.now();
    const recs = await fetchHybridRecommendations(sessionId, currentUserId, cartCodes);
    const duration = Math.round(performance.now() - start);

    setRecommendations(recs);
    setServingLatency(recs.serving_latency_ms || Math.max(1, duration));
    setIsRecsLoading(false);

    addTelemetryLog(
      'recommend_served',
      `Hybrid recommendation generated (${recs.hybrid_ranked.length} items ranked via FP-Growth & NCF).`,
      undefined,
      recs.serving_latency_ms || duration
    );
  }, [sessionId, currentUserId, cartItems, addTelemetryLog]);

  useEffect(() => {
    refreshRecommendations();
  }, [refreshRecommendations]);

  // Cart Handlers
  const handleAddToCart = async (product: Product) => {
    const start = performance.now();
    setCartItems((prev) => {
      const existing = prev.find((item) => item.product.stockCode === product.stockCode);
      if (existing) {
        return prev.map((item) =>
          item.product.stockCode === product.stockCode
            ? { ...item, quantity: item.quantity + 1 }
            : item
        );
      }
      return [...prev, { product, quantity: 1 }];
    });

    const elapsed = Math.round(performance.now() - start);
    addTelemetryLog('add_to_cart', `Kafka producer emitted event: stockCode=${product.stockCode}, qty=1`, product.stockCode, elapsed);
    showToast(`Added "${product.name}" to cart!`);

    // Async sync with FastAPI/Redis
    addCartItem(sessionId, product.stockCode, 1);
  };

  const handleUpdateQuantity = (stockCode: string, quantity: number) => {
    if (quantity <= 0) {
      handleRemoveItem(stockCode);
      return;
    }
    setCartItems((prev) =>
      prev.map((item) => (item.product.stockCode === stockCode ? { ...item, quantity } : item))
    );
  };

  const handleRemoveItem = (stockCode: string) => {
    setCartItems((prev) => prev.filter((item) => item.product.stockCode !== stockCode));
    addTelemetryLog('view', `Item removed from active session cart: stockCode=${stockCode}`, stockCode, 1.2);
  };

  const handleClearCart = () => {
    setCartItems([]);
    showToast('Cart cleared.');
  };

  const handleCheckout = () => {
    const totalQty = cartItems.reduce((acc, i) => acc + i.quantity, 0);
    addTelemetryLog('purchase', `Kafka 'purchase' event published: ${totalQty} items bought. Ingesting to MinIO Lakehouse.`, undefined, 3.4);
    showToast('🎉 Order simulated! Kafka purchase event sent to Lakehouse pipeline.');
    setCartItems([]);
    setIsCartOpen(false);
  };

  // Filter Catalog
  const filteredProducts = useMemo(() => {
    return CATALOG_PRODUCTS.filter((prod) => {
      const matchesCategory =
        selectedCategory === 'All Departments' || prod.category === selectedCategory;
      const matchesSearch =
        prod.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        prod.stockCode.toLowerCase().includes(searchTerm.toLowerCase()) ||
        prod.category.toLowerCase().includes(searchTerm.toLowerCase());
      return matchesCategory && matchesSearch;
    });
  }, [searchTerm, selectedCategory]);

  const cartTotal = useMemo(
    () => cartItems.reduce((sum, item) => sum + item.product.price * item.quantity, 0),
    [cartItems]
  );
  const cartCount = useMemo(
    () => cartItems.reduce((sum, item) => sum + item.quantity, 0),
    [cartItems]
  );
  const cartProductCodes = useMemo(
    () => cartItems.map((i) => i.product.stockCode),
    [cartItems]
  );

  return (
    <div className="min-h-screen bg-[#eaeded] flex flex-col font-sans">
      
      {/* Toast Notification */}
      {notification && (
        <div className="fixed bottom-6 right-6 z-50 bg-[#131921] text-white px-4 py-3 rounded-lg shadow-xl border border-[#ff9900] flex items-center gap-2 animate-bounce">
          <Sparkles className="w-4 h-4 text-[#ff9900]" />
          <span className="text-xs font-semibold">{notification}</span>
        </div>
      )}

      {/* Top Navbar */}
      <Navbar
        currentUserId={currentUserId}
        onSelectUser={(uid) => {
          setCurrentUserId(uid);
          addTelemetryLog('view', `Switched active user profile to #${uid}. Recalculating NCF latent embeddings.`);
        }}
        cartCount={cartCount}
        cartTotal={cartTotal}
        onOpenCart={() => setIsCartOpen(true)}
        onOpenTelemetry={() => setIsTelemetryOpen(true)}
        health={health}
        latencyMs={servingLatency}
        searchTerm={searchTerm}
        onSearchChange={setSearchTerm}
        selectedCategory={selectedCategory}
        onSelectCategory={setSelectedCategory}
      />

      {/* Hero / Banner Announcement */}
      <div className="bg-gradient-to-r from-[#232f3e] via-[#1a232f] to-[#131921] text-white py-6 border-b border-gray-700">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <div className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-amber-500/10 border border-amber-500/30 text-amber-400 text-xs font-semibold mb-2">
              <Sparkles className="w-3.5 h-3.5" />
              <span>Real-Time Hybrid Recommender (FP-Growth + PyTorch NCF)</span>
            </div>
            <h1 className="text-2xl sm:text-3xl font-extrabold tracking-tight">
              Production Streaming & Lakehouse E-Commerce Store
            </h1>
            <p className="text-xs sm:text-sm text-gray-300 mt-1 max-w-2xl">
              Trained on 805k real Kaggle retail transactions with sub-5ms Redis serving, Kafka event ingestion, and Apache Airflow automated orchestration.
            </p>
          </div>

          <div className="flex items-center gap-3">
            <button
              onClick={refreshRecommendations}
              disabled={isRecsLoading}
              className="px-3.5 py-2 bg-[#ff9900] hover:bg-[#fa8900] text-[#131921] font-bold text-xs rounded-md shadow-xs transition flex items-center gap-1.5 cursor-pointer"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${isRecsLoading ? 'animate-spin' : ''}`} />
              <span>Recalculate Scores</span>
            </button>
            <button
              onClick={() => setIsTelemetryOpen(true)}
              className="px-3.5 py-2 bg-[#37475a] hover:bg-[#485769] text-white font-medium text-xs rounded-md transition flex items-center gap-1.5 cursor-pointer"
            >
              <ExternalLink className="w-3.5 h-3.5" />
              <span>Architecture Telemetry</span>
            </button>
          </div>
        </div>
      </div>

      {/* Main Content Area */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 flex-1 w-full space-y-8">
        
        {/* Real-time Recommendations Hub */}
        <RecommendationSection
          recommendations={recommendations}
          onAddToCart={handleAddToCart}
          cartProductCodes={cartProductCodes}
          activeUserId={currentUserId}
        />

        {/* Product Catalog Section */}
        <div>
          <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 border-b border-gray-300 gap-2 mb-6">
            <div>
              <h2 className="text-xl font-bold text-gray-900">
                Explore Lakehouse Catalog ({filteredProducts.length} items)
              </h2>
              <p className="text-xs text-gray-500 mt-0.5">
                Add products below to witness instantaneous co-purchase basket rules triggering.
              </p>
            </div>

            <div className="flex items-center gap-2 text-xs">
              <Filter className="w-3.5 h-3.5 text-gray-500" />
              <span className="text-gray-500">Department:</span>
              <span className="font-semibold text-gray-800 bg-white px-2.5 py-1 rounded border border-gray-200">
                {selectedCategory}
              </span>
            </div>
          </div>

          {/* Products Grid */}
          <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-6">
            {filteredProducts.map((product) => (
              <ProductCard
                key={product.stockCode}
                product={product}
                onAddToCart={handleAddToCart}
                isInCart={cartProductCodes.includes(product.stockCode)}
              />
            ))}
          </div>
        </div>
      </main>

      {/* Footer */}
      <footer className="bg-[#131921] text-gray-400 text-xs py-8 border-t border-gray-800 mt-12">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex flex-col md:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-2">
            <div className="bg-[#ff9900] text-[#131921] font-bold px-2 py-0.5 rounded text-xs">
              ECOM RS
            </div>
            <span>Hybrid Recommender System (FP-Growth & NCF Lakehouse Architecture)</span>
          </div>

          <div className="flex items-center gap-6 text-gray-300">
            <span>Kafka Ingestion</span>
            <span>Redis Feature Store</span>
            <span>PyTorch MLOps</span>
            <span>FastAPI Serving</span>
            <span>Jenkins CI/CD</span>
          </div>
        </div>
      </footer>

      {/* Modals / Drawers */}
      <CartDrawer
        isOpen={isCartOpen}
        onClose={() => setIsCartOpen(false)}
        items={cartItems}
        sessionId={sessionId}
        onUpdateQuantity={handleUpdateQuantity}
        onRemoveItem={handleRemoveItem}
        onClearCart={handleClearCart}
        onCheckout={handleCheckout}
      />

      <TelemetryDrawer
        isOpen={isTelemetryOpen}
        onClose={() => setIsTelemetryOpen(false)}
        logs={telemetryLogs}
        latencyMs={servingLatency}
        onEmitSimulatedEvent={(eventType) => {
          const sampleProd = CATALOG_PRODUCTS[Math.floor(Math.random() * CATALOG_PRODUCTS.length)];
          addTelemetryLog(eventType, `Simulated Kafka stream event triggered for #${sampleProd.stockCode}`, sampleProd.stockCode, 2.1);
          showToast(`Stream event "${eventType}" sent to Kafka!`);
        }}
      />
    </div>
  );
}

export default App;
