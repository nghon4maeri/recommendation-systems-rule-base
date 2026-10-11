// filepath: frontend/src/types/index.ts
export interface Product {
  stockCode: string;
  name: string;
  category: string;
  price: number;
  rating: number;
  reviewCount: number;
  imageUrl?: string;
  description: string;
}

export interface CartItem {
  product: Product;
  quantity: number;
}

export interface RecommendationItem {
  stock_code: string;
  description?: string;
  score: number;
  model_type: 'FP-Growth' | 'NCF' | 'Hybrid' | 'Cold-Start';
  confidence?: number;
  lift?: number;
  rank?: number;
  match_reason?: string;
  price?: number;
}

export interface HybridRecommendationResponse {
  session_id: string;
  user_id?: string;
  cart_recommendations: RecommendationItem[];
  user_recommendations: RecommendationItem[];
  hybrid_ranked: RecommendationItem[];
  serving_latency_ms: number;
  source: string;
}

export interface TelemetryLog {
  id: string;
  timestamp: string;
  event: 'view' | 'add_to_cart' | 'purchase' | 'recommend_served' | 'cache_hit';
  stockCode?: string;
  sessionId: string;
  userId?: string;
  latencyMs: number;
  details: string;
}
