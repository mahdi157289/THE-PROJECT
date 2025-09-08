import React, { useState } from 'react';

export default function FileInspectionCard({ fileInfo, onClose }) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [previewData, setPreviewData] = useState(null);
  const [loading, setLoading] = useState(false);

  const handlePreview = async () => {
    // If already expanded, just toggle
    if (isExpanded) {
      setIsExpanded(false);
      return;
    }

    // If we have preview data, just expand
    if (previewData) {
      setIsExpanded(true);
      return;
    }

    // Load preview data
    setLoading(true);
    try {
      const response = await fetch(`http://127.0.0.1:5000/api/scraping/files/${fileInfo.filename}/preview`);
      const data = await response.json();
      
      if (data.status === 'success') {
        setPreviewData(data.data);
        setIsExpanded(true);
      } else {
        console.error('Preview failed:', data.message);
        alert(`Failed to load preview: ${data.message}`);
      }
    } catch (error) {
      console.error('Error fetching file preview:', error);
      alert('Failed to load file preview. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const formatFileSize = (bytes) => {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
  };

  const getFileIcon = (fileType) => {
    return fileType === 'indices' ? '📊' : '📈';
  };

  const getFileColor = (fileType) => {
    return fileType === 'indices' ? 'bg-blue-100 border-blue-300' : 'bg-green-100 border-green-300';
  };

  return (
    <div className={`p-4 rounded-lg border-2 ${getFileColor(fileInfo.file_type)} shadow-lg transition-all duration-300 hover:shadow-xl`}>
      {/* File Header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center space-x-3">
          <span className="text-2xl">{getFileIcon(fileInfo.file_type)}</span>
          <div>
            <h3 className="font-semibold text-gray-900">{fileInfo.filename}</h3>
            <p className="text-sm text-gray-600">
              {fileInfo.file_type.charAt(0).toUpperCase() + fileInfo.file_type.slice(1)} • Year {fileInfo.year}
            </p>
          </div>
        </div>
        <button
          onClick={onClose}
          className="text-gray-400 hover:text-gray-600 transition-colors"
        >
          ✕
        </button>
      </div>

      {/* File Details */}
      <div className="grid grid-cols-2 gap-4 mb-3 text-sm">
        <div>
          <span className="text-gray-500">Size:</span>
          <span className="ml-2 font-medium">{formatFileSize(fileInfo.size_bytes)}</span>
        </div>
        <div>
          <span className="text-gray-500">Created:</span>
          <span className="ml-2 font-medium">
            {new Date(fileInfo.created_at).toLocaleTimeString()}
          </span>
        </div>
      </div>

      {/* Preview Button */}
      <button
        onClick={handlePreview}
        disabled={loading}
        className={`w-full py-2 px-4 rounded-lg font-medium transition-colors ${
          isExpanded
            ? 'bg-red-500 text-white hover:bg-red-600'
            : 'bg-blue-600 text-white hover:bg-blue-700'
        }`}
      >
        {loading ? 'Loading...' : isExpanded ? 'Hide Preview' : 'Preview CSV'}
      </button>

      {/* CSV Preview */}
      {isExpanded && previewData && (
        <div className="mt-4 p-3 bg-white rounded-lg border">
          <h4 className="font-semibold text-gray-900 mb-2">
            CSV Preview ({previewData.total_lines} total lines)
          </h4>
          <div className="max-h-48 overflow-y-auto">
            <pre className="text-xs text-gray-700 bg-gray-50 p-2 rounded">
              {previewData.preview_lines.join('')}
            </pre>
          </div>
        </div>
      )}

      {/* Error State */}
      {isExpanded && !previewData && !loading && (
        <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg">
          <p className="text-sm text-red-700">
            ❌ Failed to load preview. Please try again.
          </p>
        </div>
      )}
    </div>
  );
}
