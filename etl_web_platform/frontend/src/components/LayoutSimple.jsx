import React from 'react';

export default function LayoutSimple({ children }) {
  return (
    <div className="min-h-screen bg-gray-50">
      {/* Simple Header */}
      <div className="bg-white shadow-sm border-b border-gray-200 p-4">
        <h1 className="text-xl font-semibold text-gray-900">Bourse de Tunis ETL Platform</h1>
      </div>
      
      {/* Main content */}
      <div className="p-6">
        {children}
      </div>
    </div>
  );
}
