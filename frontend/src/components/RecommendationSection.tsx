// filepath: frontend/src/components/RecommendationSection.tsx
import React, { useState } from 'react';
import { Sparkles, TrendingUp, Cpu, Layers, Plus, Zap, Award } from 'lucide-react';
import type { HybridRecommendationResponse, Product } from '../types';
import { CATALOG_PRODUCTS } from '../data/mockProducts';


interface RecommendationSectionProps {
  recommendations: HybridRecommendationResponse | null;
  onAddToCart: (product: Product) => void;
  cartProductCodes: string[];
  activeUserId: string;
}

export const RecommendationSection: React.FC<RecommendationSectionProps> = ({
  recommendations,
  onAddToCart,
  cartProductCodes,
  activeUserId,
}) => {
  const [activeTab, setActiveTab] = useState<'hybrid' | 'fpgrowth' | 'ncf'>('hybrid');

  if (!recommendations) {
    return null;
  }

  const findProduct = (code: string): Product => {
    return (
      CATALOG_PRODUCTS.find((p) => p.stockCode === code) || {
        stockCode: code,
        name: `Complementary Item #${code}`,
        category: 'Lakehouse Catalog',
        price: 2.95,
        rating: 4.8,
        reviewCount: 540,
        imageUrl: 'https://images.unsplash.com/photo-1544816155-12df9643f363?w=500&auto=format&fit=crop&q=80',
        description: 'Auto-retrieved through real-time association rule engine.',
      }
    );
  };

  const getDisplayItems = () => {
    switch (activeTab) {
      case 'fpgrowth':
        return recommendations.cart_recommendations;
      case 'ncf':
        return recommendations.user_recommendations;
      case 'hybrid':
      default:
        return recommendations.hybrid_ranked;
    }
  };

  const items = getDisplayItems();

  return (
    <section className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm mb-8">
      {/* Header and Controls */}
      <div className="flex flex-col md:flex-row md:items-center justify-between pb-4 border-b border-gray-200 gap-4">
        <div>
          <div className="flex items-center gap-2">
            <Sparkles className="w-5 h-5 text-amber-500 fill-amber-500" />
            <h2 className="text-lg font-bold text-gray-900">
              AI Intelligent Recommendation Engine
            </h2>
            <span className="bg-emerald-100 text-emerald-800 text-[11px] font-mono font-bold px-2 py-0.5 rounded-full flex items-center gap-1">
              <Zap className="w-3 h-3 text-emerald-600" />
              {recommendations.serving_latency_ms} ms Latency
            </span>
          </div>
          <p className="text-xs text-gray-500 mt-0.5">
            Engine Source: <span className="font-semibold text-gray-700">{recommendations.source}</span>
          </p>
        </div>

        {/* Algorithm Tabs */}
        <div className="flex items-center bg-gray-100 p-1 rounded-lg self-start md:self-auto text-xs font-semibold">
          <button
            onClick={() => setActiveTab('hybrid')}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md transition cursor-pointer ${
              activeTab === 'hybrid'
                ? 'bg-white text-gray-900 shadow-xs font-bold border border-gray-200'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            <Layers className="w-3.5 h-3.5 text-blue-600" />
            <span>Hybrid Ensemble</span>
          </button>

          <button
            onClick={() => setActiveTab('fpgrowth')}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md transition cursor-pointer ${
              activeTab === 'fpgrowth'
                ? 'bg-white text-gray-900 shadow-xs font-bold border border-gray-200'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            <TrendingUp className="w-3.5 h-3.5 text-amber-600" />
            <span>Frequently Bought (FP-Growth)</span>
            <span className="bg-amber-100 text-amber-800 text-[10px] px-1.5 py-0.2 rounded-full">
              {recommendations.cart_recommendations.length}
            </span>
          </button>

          <button
            onClick={() => setActiveTab('ncf')}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md transition cursor-pointer ${
              activeTab === 'ncf'
                ? 'bg-white text-gray-900 shadow-xs font-bold border border-gray-200'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            <Cpu className="w-3.5 h-3.5 text-purple-600" />
            <span>Personalized (PyTorch NCF)</span>
            <span className="bg-purple-100 text-purple-800 text-[10px] px-1.5 py-0.2 rounded-full">
              {recommendations.user_recommendations.length}
            </span>
          </button>
        </div>
      </div>

      {/* Algorithm Strategy Context Banner */}
      <div className="mt-4 p-3 rounded-lg bg-gray-50 border border-gray-200 flex items-center justify-between text-xs text-gray-600">
        <div className="flex items-center gap-2">
          {activeTab === 'hybrid' && (
            <>
              <Award className="w-4 h-4 text-blue-600 shrink-0" />
              <span>
                <strong>Weighted Hybrid Blending:</strong> Combines real-time cart co-occurrence (weight 0.65) with deep collaborative filtering embeddings (weight 0.35) for user #{activeUserId}.
              </span>
            </>
          )}
          {activeTab === 'fpgrowth' && (
            <>
              <TrendingUp className="w-4 h-4 text-amber-600 shrink-0" />
              <span>
                <strong>FP-Growth Association Mining:</strong> Mined from 805,000 Lakehouse baskets with sub-second $O(1)$ key lookups in Redis Feature Store.
              </span>
            </>
          )}
          {activeTab === 'ncf' && (
            <>
              <Cpu className="w-4 h-4 text-purple-600 shrink-0" />
              <span>
                <strong>Neural Collaborative Filtering:</strong> 32-dimensional latent embedding dot-product between User #{activeUserId} and candidate items.
              </span>
            </>
          )}
        </div>
        <div className="text-[11px] font-mono text-gray-400 shrink-0 hidden sm:block">
          Redis Key: <code className="text-gray-700 bg-gray-200 px-1 py-0.5 rounded">rules:stock_code</code>
        </div>
      </div>

      {/* Items Cards Grid */}
      {items.length === 0 ? (
        <div className="py-12 text-center text-gray-400">
          <p className="text-sm">No items currently triggered for this rule combination.</p>
          <p className="text-xs mt-1">Try adding products like "Jumbo Bag Red Retrospot" or "Lunch Bag" to the cart to trigger association rules.</p>
        </div>
      ) : (
        <div className="mt-6 grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
          {items.map((item, idx) => {
            const prod = findProduct(item.stock_code);
            const inCart = cartProductCodes.includes(item.stock_code);

            return (
              <div
                key={`${item.stock_code}-${idx}`}
                className="border border-gray-200 rounded-lg p-3 hover:border-amber-400 hover:shadow-md transition bg-white flex flex-col justify-between relative group"
              >
                {/* Ranking or Model Badge */}
                <div className="flex items-center justify-between mb-2">
                  <span className="text-[10px] font-mono text-gray-400">
                    StockCode: #{item.stock_code}
                  </span>
                  {item.lift ? (
                    <span className="bg-amber-100 text-amber-800 text-[10px] font-bold px-1.5 py-0.5 rounded">
                      Lift {item.lift}x
                    </span>
                  ) : (
                    <span className="bg-blue-100 text-blue-800 text-[10px] font-bold px-1.5 py-0.5 rounded">
                      Score {(item.score * 100).toFixed(0)}%
                    </span>
                  )}
                </div>

                {/* Thumbnail and Title */}
                <div className="flex gap-3 items-center">
                  <img
                    src={prod.imageUrl}
                    alt={prod.name}
                    className="w-16 h-16 rounded object-cover bg-gray-100 shrink-0"
                  />
                  <div className="min-w-0 flex-1">
                    <h4 className="text-xs font-bold text-gray-900 truncate group-hover:text-amber-600 transition">
                      {prod.name}
                    </h4>
                    <p className="text-[11px] text-gray-500 mt-0.5 line-clamp-1">
                      {item.match_reason || prod.category}
                    </p>
                    <div className="mt-1 flex items-baseline gap-1">
                      <span className="text-xs text-gray-600 font-semibold">£{prod.price.toFixed(2)}</span>
                    </div>
                  </div>
                </div>

                {/* Score bar indicator */}
                <div className="mt-3">
                  <div className="flex justify-between text-[10px] text-gray-400 mb-1">
                    <span>Model Affinity</span>
                    <span className="font-mono font-semibold text-gray-600">
                      {item.confidence ? `${(item.confidence * 100).toFixed(0)}% Conf` : `${(item.score * 100).toFixed(0)}% Match`}
                    </span>
                  </div>
                  <div className="w-full bg-gray-100 rounded-full h-1.5 overflow-hidden">
                    <div
                      className={`h-1.5 rounded-full ${
                        item.model_type === 'FP-Growth' ? 'bg-amber-500' : item.model_type === 'NCF' ? 'bg-purple-500' : 'bg-blue-600'
                      }`}
                      style={{ width: `${Math.min(100, (item.confidence || item.score) * 100)}%` }}
                    />
                  </div>
                </div>

                {/* Add to Cart CTA */}
                <button
                  onClick={() => onAddToCart(prod)}
                  disabled={inCart}
                  className={`mt-3 w-full py-1.5 px-2 rounded text-xs font-semibold flex items-center justify-center gap-1 transition cursor-pointer ${
                    inCart
                      ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
                      : 'bg-[#ffd814] hover:bg-[#f7ca00] text-gray-900 border border-[#fcd200]'
                  }`}
                >
                  {inCart ? (
                    'Already in Cart'
                  ) : (
                    <>
                      <Plus className="w-3.5 h-3.5" /> Quick Add
                    </>
                  )}
                </button>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
};
