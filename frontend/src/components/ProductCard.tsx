// filepath: frontend/src/components/ProductCard.tsx
import React, { useState } from 'react';
import { Star, Plus, Check, ShoppingBag } from 'lucide-react';
import type { Product } from '../types';


interface ProductCardProps {
  product: Product;
  onAddToCart: (product: Product) => void;
  isInCart: boolean;
}

export const ProductCard: React.FC<ProductCardProps> = ({ product, onAddToCart, isInCart }) => {
  const [isAdding, setIsAdding] = useState(false);

  const handleAdd = () => {
    setIsAdding(true);
    onAddToCart(product);
    setTimeout(() => setIsAdding(false), 600);
  };

  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden shadow-sm hover:shadow-md transition-shadow flex flex-col justify-between group">
      <div>
        {/* Product Image */}
        <div className="relative aspect-square w-full overflow-hidden bg-gray-100">
          <img
            src={product.imageUrl}
            alt={product.name}
            className="w-full h-full object-cover object-center group-hover:scale-105 transition-transform duration-300"
            loading="lazy"
          />
          <div className="absolute top-2 left-2 bg-[#131921]/80 backdrop-blur-xs text-white text-[11px] font-mono px-2 py-0.5 rounded shadow">
            #{product.stockCode}
          </div>
          <div className="absolute top-2 right-2 bg-amber-100 text-amber-800 text-[10px] font-semibold px-2 py-0.5 rounded-full uppercase tracking-wider">
            {product.category}
          </div>
        </div>

        {/* Product Details */}
        <div className="p-4">
          <h3 className="text-sm font-semibold text-gray-900 line-clamp-2 min-h-[40px] group-hover:text-blue-700 transition-colors">
            {product.name}
          </h3>

          <p className="mt-1 text-xs text-gray-500 line-clamp-2">
            {product.description}
          </p>

          {/* Ratings & Review Count */}
          <div className="flex items-center gap-1.5 mt-2">
            <div className="flex items-center text-amber-400">
              <Star className="w-3.5 h-3.5 fill-current" />
              <span className="text-xs font-bold text-gray-700 ml-1">{product.rating}</span>
            </div>
            <span className="text-[11px] text-gray-400">({product.reviewCount.toLocaleString()} ratings)</span>
          </div>

          {/* Pricing */}
          <div className="mt-3 flex items-baseline gap-1">
            <span className="text-xs text-gray-600 font-medium">£</span>
            <span className="text-xl font-bold text-gray-900">{product.price.toFixed(2)}</span>
            <span className="text-[11px] text-emerald-600 ml-1 font-medium">In Stock</span>
          </div>
        </div>
      </div>

      {/* Action Footer */}
      <div className="p-4 pt-0">
        <button
          onClick={handleAdd}
          disabled={isAdding}
          className={`w-full py-2 px-3 rounded-md text-xs font-bold flex items-center justify-center gap-1.5 transition-colors cursor-pointer shadow-xs ${
            isAdding || isInCart
              ? 'bg-emerald-600 hover:bg-emerald-700 text-white'
              : 'bg-[#ffd814] hover:bg-[#f7ca00] text-gray-900 border border-[#fcd200]'
          }`}
        >
          {isAdding ? (
            <>
              <Check className="w-4 h-4 animate-scale" /> Added to Session!
            </>
          ) : isInCart ? (
            <>
              <ShoppingBag className="w-4 h-4" /> Add More to Cart
            </>
          ) : (
            <>
              <Plus className="w-4 h-4" /> Add to Cart
            </>
          )}
        </button>
      </div>
    </div>
  );
};
