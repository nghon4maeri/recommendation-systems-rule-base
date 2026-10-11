// filepath: frontend/src/components/CartDrawer.tsx
import React from 'react';
import { X, Trash2, Plus, Minus, ShoppingBag, ShieldCheck, ArrowRight, Database } from 'lucide-react';
import type { CartItem } from '../types';


interface CartDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  items: CartItem[];
  sessionId: string;
  onUpdateQuantity: (stockCode: string, quantity: number) => void;
  onRemoveItem: (stockCode: string) => void;
  onClearCart: () => void;
  onCheckout: () => void;
}

export const CartDrawer: React.FC<CartDrawerProps> = ({
  isOpen,
  onClose,
  items,
  sessionId,
  onUpdateQuantity,
  onRemoveItem,
  onClearCart,
  onCheckout,
}) => {
  if (!isOpen) return null;

  const total = items.reduce((sum, item) => sum + item.product.price * item.quantity, 0);
  const itemCount = items.reduce((sum, item) => sum + item.quantity, 0);

  return (
    <div className="fixed inset-0 z-50 overflow-hidden">
      {/* Backdrop */}
      <div
        onClick={onClose}
        className="absolute inset-0 bg-black/50 backdrop-blur-xs transition-opacity duration-300"
      />

      <div className="fixed inset-y-0 right-0 max-w-full flex pl-10">
        <div className="w-screen max-w-md bg-white shadow-2xl flex flex-col justify-between">
          
          {/* Drawer Header */}
          <div className="p-4 bg-[#131921] text-white flex items-center justify-between border-b border-gray-700">
            <div className="flex items-center gap-2">
              <ShoppingBag className="w-5 h-5 text-amber-400" />
              <h2 className="text-base font-bold">Shopping Cart ({itemCount})</h2>
            </div>
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-white transition p-1 cursor-pointer"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Redis Session Meta Banner */}
          <div className="bg-gray-100 px-4 py-2 border-b border-gray-200 text-xs flex items-center justify-between text-gray-600">
            <div className="flex items-center gap-1.5">
              <Database className="w-3.5 h-3.5 text-blue-600" />
              <span>Redis Session:</span>
              <span className="font-mono font-bold text-gray-800">{sessionId}</span>
            </div>
            <span className="bg-emerald-100 text-emerald-800 text-[10px] font-semibold px-2 py-0.5 rounded">
              TTL: 3600s
            </span>
          </div>

          {/* Cart Item List */}
          <div className="flex-1 overflow-y-auto p-4 space-y-4">
            {items.length === 0 ? (
              <div className="text-center py-16 text-gray-400">
                <ShoppingBag className="w-12 h-12 mx-auto stroke-1 text-gray-300 mb-3" />
                <p className="text-sm font-semibold text-gray-600">Your basket is empty</p>
                <p className="text-xs text-gray-400 mt-1">
                  Add items to your basket to generate real-time FP-Growth complementary recommendations!
                </p>
              </div>
            ) : (
              items.map((item) => (
                <div
                  key={item.product.stockCode}
                  className="flex gap-3 p-3 bg-white border border-gray-200 rounded-lg shadow-2xs hover:border-gray-300 transition"
                >
                  <img
                    src={item.product.imageUrl}
                    alt={item.product.name}
                    className="w-16 h-16 rounded object-cover bg-gray-100 shrink-0"
                  />

                  <div className="flex-1 min-w-0">
                    <div className="flex justify-between items-start gap-2">
                      <h4 className="text-xs font-bold text-gray-900 truncate">
                        {item.product.name}
                      </h4>
                      <button
                        onClick={() => onRemoveItem(item.product.stockCode)}
                        className="text-gray-400 hover:text-red-500 transition cursor-pointer"
                        title="Remove item"
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    </div>

                    <p className="text-[11px] font-mono text-gray-400 mt-0.5">
                      #{item.product.stockCode} · {item.product.category}
                    </p>

                    <div className="mt-2 flex items-center justify-between">
                      {/* Quantity Controls */}
                      <div className="flex items-center border border-gray-300 rounded overflow-hidden">
                        <button
                          onClick={() => onUpdateQuantity(item.product.stockCode, item.quantity - 1)}
                          className="px-2 py-0.5 bg-gray-100 hover:bg-gray-200 text-gray-600 transition cursor-pointer"
                        >
                          <Minus className="w-3 h-3" />
                        </button>
                        <span className="px-2.5 py-0.5 text-xs font-bold text-gray-800">
                          {item.quantity}
                        </span>
                        <button
                          onClick={() => onUpdateQuantity(item.product.stockCode, item.quantity + 1)}
                          className="px-2 py-0.5 bg-gray-100 hover:bg-gray-200 text-gray-600 transition cursor-pointer"
                        >
                          <Plus className="w-3 h-3" />
                        </button>
                      </div>

                      <div className="text-xs font-bold text-gray-900">
                        £{(item.product.price * item.quantity).toFixed(2)}
                      </div>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>

          {/* Drawer Footer & Checkout */}
          {items.length > 0 && (
            <div className="p-4 bg-gray-50 border-t border-gray-200 space-y-3">
              <div className="flex justify-between text-xs text-gray-600">
                <span>Subtotal ({itemCount} items)</span>
                <span className="font-bold text-gray-900 text-base">£{total.toFixed(2)}</span>
              </div>

              <div className="flex justify-between text-[11px] text-gray-500">
                <span>Standard Delivery</span>
                <span className="text-emerald-600 font-semibold">FREE (Prime Eligible)</span>
              </div>

              <button
                onClick={onCheckout}
                className="w-full py-2.5 px-4 bg-[#ffd814] hover:bg-[#f7ca00] text-gray-900 font-bold rounded-lg transition-colors border border-[#fcd200] flex items-center justify-center gap-2 cursor-pointer shadow-xs text-sm"
              >
                <span>Checkout (Simulate Kafka Purchase)</span>
                <ArrowRight className="w-4 h-4" />
              </button>

              <div className="flex items-center justify-between pt-1">
                <button
                  onClick={onClearCart}
                  className="text-xs text-gray-400 hover:text-red-600 transition cursor-pointer"
                >
                  Clear all items
                </button>
                <div className="flex items-center gap-1 text-[11px] text-gray-500">
                  <ShieldCheck className="w-3.5 h-3.5 text-emerald-600" />
                  <span>Lakehouse Verified</span>
                </div>
              </div>
            </div>
          )}

        </div>
      </div>
    </div>
  );
};
