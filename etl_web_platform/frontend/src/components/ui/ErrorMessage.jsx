import React from 'react';
import { HiExclamationTriangle, HiRefresh } from 'react-icons/hi';

/**
 * Error Message Component
 * @param {Object} props - Component props
 * @param {string} props.error - Error message to display
 * @param {Function} props.onRetry - Retry function
 * @param {string} props.title - Optional error title
 * @param {string} props.className - Additional CSS classes
 */
export function ErrorMessage({ error, onRetry, title = 'Error Occurred', className = '' }) {
  if (!error) return null;

  return (
    <div className={`bg-red-50 border border-red-200 rounded-lg p-4 ${className}`}>
      <div className="flex items-start">
        <div className="flex-shrink-0">
          <HiExclamationTriangle className="h-5 w-5 text-red-400" />
        </div>
        <div className="ml-3 flex-1">
          <h3 className="text-sm font-medium text-red-800">{title}</h3>
          <div className="mt-2 text-sm text-red-700">
            <p>{error}</p>
          </div>
          {onRetry && (
            <div className="mt-4">
              <button
                type="button"
                onClick={onRetry}
                className="inline-flex items-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-red-700 bg-red-100 hover:bg-red-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500 transition-colors"
              >
                <HiRefresh className="h-4 w-4 mr-2" />
                Try Again
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

/**
 * Connection Error Component for API connectivity issues
 * @param {Object} props - Component props
 * @param {Function} props.onRetry - Retry connection function
 * @param {string} props.className - Additional CSS classes
 */
export function ConnectionError({ onRetry, className = '' }) {
  return (
    <div className={`bg-yellow-50 border border-yellow-200 rounded-lg p-4 ${className}`}>
      <div className="flex items-start">
        <div className="flex-shrink-0">
          <HiExclamationTriangle className="h-5 w-5 text-yellow-400" />
        </div>
        <div className="ml-3 flex-1">
          <h3 className="text-sm font-medium text-yellow-800">Connection Lost</h3>
          <div className="mt-2 text-sm text-yellow-700">
            <p>Unable to connect to the backend server. Please check your connection and try again.</p>
          </div>
          {onRetry && (
            <div className="mt-4">
              <button
                type="button"
                onClick={onRetry}
                className="inline-flex items-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-yellow-700 bg-yellow-100 hover:bg-yellow-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-yellow-500 transition-colors"
              >
                <HiRefresh className="h-4 w-4 mr-2" />
                Reconnect
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
