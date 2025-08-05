"""
FIXED Deep Learning Models for Market Analysis
==============================================

FIXES APPLIED:
1. TensorFlow History object handling - proper access to training history
2. Enhanced model architectures with better regularization
3. Improved training callbacks and monitoring
4. Better error handling and logging
5. Model performance evaluation and metrics

PURPOSE:
- Builds CNN-LSTM hybrid models for time series prediction
- Implements Transformer architecture for market forecasting
- Provides comprehensive training monitoring
- Includes model evaluation and performance metrics

USAGE:
Replace the original deep_models.py with this fixed version
"""

import numpy as np
import pandas as pd
import logging
from typing import Tuple, Dict, Any, Optional
import warnings

# Suppress TensorFlow warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning, module='tensorflow')

try:
    import tensorflow as tf
    from tensorflow.keras.models import Model, Sequential
    from tensorflow.keras.layers import (
        Conv1D, LSTM, Dense, Flatten, Input, Dropout, BatchNormalization,
        Lambda, LayerNormalization, MultiHeadAttention, Add
    )
    from tensorflow.keras.optimizers import Adam
    from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
    from tensorflow.keras.regularizers import l2
    
    # Configure TensorFlow for better performance
    tf.keras.utils.disable_interactive_logging()
    
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False
    tf = None

logger = logging.getLogger('DeepModels')

class MarketModelBuilder:
    """
    FIXED: Enhanced deep learning model builder with proper training handling
    """
    
    def __init__(self, lookback_window: int = 60, validation_split: float = 0.2):
        """
        Initialize model builder with enhanced configuration
        
        Args:
            lookback_window: Number of time steps to look back for prediction
            validation_split: Fraction of data to use for validation
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is not available. Install with: pip install tensorflow")
        
        self.lookback_window = lookback_window
        self.validation_split = validation_split
        self.logger = logger
        self._configure_logger()
        
        # Model tracking
        self.trained_models = {}
        self.training_histories = {}
    
    def _configure_logger(self):
        """Configure logger with proper formatting"""
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _create_sequences(self, data: np.ndarray, target_column: int = 0) -> Tuple[np.ndarray, np.ndarray]:
        """
        Create input-output sequences from time series data
        
        ENHANCED: Better sequence creation with multiple features support
        
        Args:
            data: Time series data (can be multivariate)
            target_column: Index of column to predict (for multivariate data)
        
        Returns:
            X: Input sequences of shape (samples, timesteps, features)
            y: Target values of shape (samples,)
        """
        if len(data.shape) == 1:
            data = data.reshape(-1, 1)
        
        X, y = [], []
        
        for i in range(len(data) - self.lookback_window):
            # Input sequence: all features for lookback_window steps
            X.append(data[i:i + self.lookback_window])
            # Target: specific column at next time step
            y.append(data[i + self.lookback_window, target_column])
        
        X = np.array(X)
        y = np.array(y)
        
        self.logger.debug(f"Created {len(X)} sequences with shape {X.shape}")
        return X, y
    
    def _get_callbacks(self, model_name: str) -> list:
        """
        Create training callbacks for better model training
        
        PURPOSE: Prevents overfitting and improves training stability
        """
        callbacks = [
            EarlyStopping(
                monitor='val_loss',
                patience=10,
                restore_best_weights=True,
                verbose=0
            ),
            ReduceLROnPlateau(
                monitor='val_loss',
                factor=0.5,
                patience=5,
                min_lr=1e-7,
                verbose=0
            ),
            TrainingLogger(self.logger, model_name)
        ]
        
        return callbacks
    
    def build_cnn_lstm(self, input_shape: Tuple[int, int], 
                      conv_filters: int = 64, 
                      lstm_units: int = 100,
                      dropout_rate: float = 0.2) -> Model:
        """
        ENHANCED: CNN-LSTM hybrid model with improved architecture
        
        IMPROVEMENTS:
        - Added BatchNormalization for training stability
        - Dropout layers for regularization
        - L2 regularization to prevent overfitting
        - Better layer configuration
        """
        model = Sequential([
            Input(shape=input_shape, name='input_layer'),
            
            # Convolutional layers for feature extraction
            Conv1D(
                filters=conv_filters,
                kernel_size=3,
                activation='relu',
                kernel_regularizer=l2(0.001),
                name='conv1d_1'
            ),
            BatchNormalization(name='batch_norm_1'),
            Dropout(dropout_rate, name='dropout_1'),
            
            Conv1D(
                filters=conv_filters,
                kernel_size=3,
                activation='relu',
                kernel_regularizer=l2(0.001),
                name='conv1d_2'
            ),
            BatchNormalization(name='batch_norm_2'),
            Dropout(dropout_rate, name='dropout_2'),
            
            # LSTM layers for sequence modeling
            LSTM(
                units=lstm_units,
                return_sequences=True,
                kernel_regularizer=l2(0.001),
                name='lstm_1'
            ),
            Dropout(dropout_rate, name='dropout_3'),
            
            LSTM(
                units=lstm_units,
                kernel_regularizer=l2(0.001),
                name='lstm_2'
            ),
            Dropout(dropout_rate, name='dropout_4'),
            
            # Output layer
            Dense(50, activation='relu', kernel_regularizer=l2(0.001), name='dense_1'),
            Dropout(dropout_rate, name='dropout_5'),
            Dense(1, name='output_layer')
        ])
        
        # Compile model with appropriate optimizer and metrics
        model.compile(
            optimizer=Adam(learning_rate=0.001),
            loss='mse',
            metrics=['mae', 'mape']
        )
        
        self.logger.info(f"Built CNN-LSTM model with {model.count_params():,} parameters")
        return model
    
    def build_transformer(self, input_shape: Tuple[int, int], 
                         num_heads: int = 4, 
                         ff_dim: int = 64,
                         num_transformer_blocks: int = 2,
                         dropout_rate: float = 0.1) -> Model:
        """
        ENHANCED: Transformer model with proper attention mechanism
        
        IMPROVEMENTS:
        - Multiple transformer blocks
        - Proper residual connections
        - Enhanced positional encoding
        - Better regularization
        """
        inputs = Input(shape=input_shape, name='input_layer')
        
        # Positional encoding (simplified but effective)
        x = self._add_positional_encoding(inputs)
        
        # Multiple Transformer blocks
        for i in range(num_transformer_blocks):
            x = self._transformer_block(
                x, 
                num_heads=num_heads, 
                ff_dim=ff_dim, 
                dropout_rate=dropout_rate,
                block_name=f'transformer_block_{i+1}'
            )
        
        # Global average pooling instead of flatten for better generalization
        x = tf.keras.layers.GlobalAveragePooling1D(name='global_avg_pool')(x)
        
        # Output layers with regularization
        x = Dense(ff_dim, activation='relu', kernel_regularizer=l2(0.001), name='dense_1')(x)
        x = Dropout(dropout_rate, name='dropout_final')(x)
        outputs = Dense(1, name='output_layer')(x)
        
        model = Model(inputs=inputs, outputs=outputs, name='transformer_model')
        
        # Compile with appropriate optimizer and metrics
        model.compile(
            optimizer=Adam(learning_rate=0.001),
            loss='mse',
            metrics=['mae', 'mape']
        )
        
        self.logger.info(f"Built Transformer model with {model.count_params():,} parameters")
        return model
    
    def _add_positional_encoding(self, inputs):
        """Add positional encoding to input embeddings"""
        seq_len = inputs.shape[1]
        d_model = inputs.shape[2]
        
        # Create positional encoding
        position = tf.range(seq_len, dtype=tf.float32)[:, tf.newaxis]
        div_term = tf.exp(tf.range(0, d_model, 2, dtype=tf.float32) * 
                         -(tf.math.log(10000.0) / d_model))
        
        pos_encoding = tf.zeros((seq_len, d_model))
        pos_encoding = tf.tensor_scatter_nd_update(
            pos_encoding,
            tf.stack([tf.range(seq_len), tf.range(0, d_model, 2)], axis=1),
            tf.sin(position * div_term)
        )
        if d_model > 1:
            pos_encoding = tf.tensor_scatter_nd_update(
                pos_encoding,
                tf.stack([tf.range(seq_len), tf.range(1, d_model, 2)], axis=1),
                tf.cos(position * div_term)
            )
        
        return inputs + pos_encoding
    
    def _transformer_block(self, inputs, num_heads: int, ff_dim: int, 
                          dropout_rate: float, block_name: str):
        """
        Single transformer block with multi-head attention and feed-forward network
        """
        # Multi-head self-attention
        attention_output = MultiHeadAttention(
            num_heads=num_heads,
            key_dim=inputs.shape[-1],
            name=f'{block_name}_attention'
        )(inputs, inputs)
        
        attention_output = Dropout(dropout_rate, name=f'{block_name}_dropout_1')(attention_output)
        
        # Add & Norm
        out1 = Add(name=f'{block_name}_add_1')([inputs, attention_output])
        out1 = LayerNormalization(epsilon=1e-6, name=f'{block_name}_norm_1')(out1)
        
        # Feed-forward network
        ffn_output = Dense(ff_dim, activation='relu', name=f'{block_name}_ffn_1')(out1)
        ffn_output = Dropout(dropout_rate, name=f'{block_name}_dropout_2')(ffn_output)
        ffn_output = Dense(inputs.shape[-1], name=f'{block_name}_ffn_2')(ffn_output)
        
        # Add & Norm
        out2 = Add(name=f'{block_name}_add_2')([out1, ffn_output])
        out2 = LayerNormalization(epsilon=1e-6, name=f'{block_name}_norm_2')(out2)
        
        return out2
    
    def train_model(self, model: Model, 
                   X_train: np.ndarray, 
                   y_train: np.ndarray,
                   epochs: int = 50, 
                   batch_size: int = 32,
                   model_name: str = "model") -> Dict[str, Any]:
        """
        FIXED: Train model with proper history handling and comprehensive metrics
        
        FIXES:
        - Returns training results instead of modified model
        - Proper access to training history
        - Enhanced validation and monitoring
        - Comprehensive performance metrics
        
        Returns:
            Dictionary containing training results and metrics
        """
        try:
            self.logger.info(f"Starting training for {model_name}")
            self.logger.info(f"Training data shape: {X_train.shape}, Target shape: {y_train.shape}")
            
            # Get callbacks for training
            callbacks = self._get_callbacks(model_name)
            
            # Train the model
            history = model.fit(
                X_train, y_train,
                epochs=epochs,
                batch_size=batch_size,
                validation_split=self.validation_split,
                callbacks=callbacks,
                verbose=0,  # Suppress default output (we have custom logging)
                shuffle=True
            )
            
            # FIXED: Store training history properly
            training_history = history.history
            self.training_histories[model_name] = training_history
            
            # Calculate final metrics
            final_train_loss = float(training_history['loss'][-1])
            final_val_loss = float(training_history['val_loss'][-1])
            final_train_mae = float(training_history.get('mae', [0])[-1])
            final_val_mae = float(training_history.get('val_mae', [0])[-1])
            
            # Model evaluation on training data
            train_metrics = model.evaluate(X_train, y_train, verbose=0)
            
            # Create validation split for additional evaluation
            split_index = int(len(X_train) * (1 - self.validation_split))
            X_val = X_train[split_index:]
            y_val = y_train[split_index:]
            
            val_metrics = model.evaluate(X_val, y_val, verbose=0) if len(X_val) > 0 else train_metrics
            
            # Calculate additional performance metrics
            y_pred_train = model.predict(X_train, verbose=0).flatten()
            y_pred_val = model.predict(X_val, verbose=0).flatten() if len(X_val) > 0 else y_pred_train
            
            # Calculate R² score
            train_r2 = self._calculate_r2(y_train, y_pred_train)
            val_r2 = self._calculate_r2(y_val, y_pred_val) if len(X_val) > 0 else train_r2
            
            # Determine training quality
            overfitting_ratio = final_val_loss / final_train_loss if final_train_loss > 0 else 1.0
            training_quality = self._assess_training_quality(overfitting_ratio, final_val_loss)
            
            # Store trained model
            self.trained_models[model_name] = model
            
            # Comprehensive training results
            training_results = {
                'model_name': model_name,
                'training_completed': True,
                'epochs_trained': len(training_history['loss']),
                'final_metrics': {
                    'train_loss': final_train_loss,
                    'val_loss': final_val_loss,
                    'train_mae': final_train_mae,
                    'val_mae': final_val_mae,
                    'train_r2': float(train_r2),
                    'val_r2': float(val_r2)
                },
                'model_info': {
                    'total_parameters': int(model.count_params()),
                    'trainable_parameters': int(sum([tf.keras.backend.count_params(w) for w in model.trainable_weights])),
                    'model_architecture': model.name,
                    'input_shape': list(X_train.shape[1:]),
                    'output_shape': [1]
                },
                'training_config': {
                    'epochs': epochs,
                    'batch_size': batch_size,
                    'validation_split': self.validation_split,
                    'optimizer': model.optimizer.get_config()['name'],
                    'learning_rate': float(tf.keras.backend.get_value(model.optimizer.learning_rate))
                },
                'performance_assessment': {
                    'overfitting_ratio': float(overfitting_ratio),
                    'training_quality': training_quality,
                    'convergence_achieved': final_val_loss < 1.0,  # Threshold can be adjusted
                    'improvement_over_epochs': float((training_history['val_loss'][0] - final_val_loss) / training_history['val_loss'][0])
                },
                'training_history_summary': {
                    'best_val_loss': float(min(training_history['val_loss'])),
                    'best_epoch': int(np.argmin(training_history['val_loss']) + 1),
                    'final_epoch': len(training_history['loss']),
                    'early_stopped': len(training_history['loss']) < epochs
                }
            }
            
            self.logger.info(
                f"Training completed for {model_name}: "
                f"Final Val Loss: {final_val_loss:.6f}, "
                f"Val R²: {val_r2:.4f}, "
                f"Quality: {training_quality}"
            )
            
            return training_results
            
        except Exception as e:
            self.logger.error(f"Training failed for {model_name}: {str(e)}")
            return {
                'model_name': model_name,
                'training_completed': False,
                'error': str(e),
                'error_type': type(e).__name__
            }
    
    def _calculate_r2(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Calculate R² (coefficient of determination) score"""
        try:
            ss_res = np.sum((y_true - y_pred) ** 2)
            ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
            r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0.0
            return max(-1.0, min(1.0, r2))  # Clamp between -1 and 1
        except Exception:
            return 0.0
    
    def _assess_training_quality(self, overfitting_ratio: float, val_loss: float) -> str:
        """
        Assess the quality of model training based on metrics
        
        Returns: 'excellent', 'good', 'fair', 'poor', or 'overfitting'
        """
        if overfitting_ratio > 2.0:
            return 'overfitting'
        elif val_loss < 0.001 and overfitting_ratio < 1.2:
            return 'excellent'
        elif val_loss < 0.01 and overfitting_ratio < 1.5:
            return 'good'
        elif val_loss < 0.1 and overfitting_ratio < 2.0:
            return 'fair'
        else:
            return 'poor'
    
    def predict_with_model(self, model_name: str, X_test: np.ndarray) -> Dict[str, Any]:
        """
        Make predictions using a trained model
        
        Args:
            model_name: Name of the trained model to use
            X_test: Test data for prediction
            
        Returns:
            Dictionary containing predictions and confidence metrics
        """
        if model_name not in self.trained_models:
            return {'error': f'Model {model_name} not found in trained models'}
        
        try:
            model = self.trained_models[model_name]
            
            # Make predictions
            predictions = model.predict(X_test, verbose=0)
            predictions_flat = predictions.flatten()
            
            # Calculate prediction confidence (based on model uncertainty)
            # This is a simple approach - more sophisticated methods exist
            prediction_std = float(np.std(predictions_flat))
            prediction_mean = float(np.mean(predictions_flat))
            
            return {
                'model_name': model_name,
                'predictions': predictions_flat.tolist(),
                'prediction_stats': {
                    'mean': prediction_mean,
                    'std': prediction_std,
                    'min': float(np.min(predictions_flat)),
                    'max': float(np.max(predictions_flat)),
                    'count': len(predictions_flat)
                },
                'confidence_metrics': {
                    'prediction_uncertainty': prediction_std,
                    'relative_uncertainty': float(prediction_std / abs(prediction_mean)) if prediction_mean != 0 else float('inf')
                }
            }
            
        except Exception as e:
            return {'error': f'Prediction failed: {str(e)}'}
    
    def get_model_summary(self, model_name: str) -> Dict[str, Any]:
        """
        Get comprehensive summary of a trained model
        
        Returns:
            Dictionary containing model architecture, training history, and performance
        """
        if model_name not in self.trained_models:
            return {'error': f'Model {model_name} not found'}
        
        model = self.trained_models[model_name]
        history = self.training_histories.get(model_name, {})
        
        # Get model architecture summary
        summary_lines = []
        model.summary(print_fn=lambda x: summary_lines.append(x))
        architecture_summary = '\n'.join(summary_lines)
        
        return {
            'model_name': model_name,
            'architecture_summary': architecture_summary,
            'parameter_count': {
                'total': int(model.count_params()),
                'trainable': int(sum([tf.keras.backend.count_params(w) for w in model.trainable_weights])),
                'non_trainable': int(sum([tf.keras.backend.count_params(w) for w in model.non_trainable_weights]))
            },
            'training_history': {
                'epochs_completed': len(history.get('loss', [])),
                'final_train_loss': float(history['loss'][-1]) if history.get('loss') else None,
                'final_val_loss': float(history['val_loss'][-1]) if history.get('val_loss') else None,
                'best_val_loss': float(min(history['val_loss'])) if history.get('val_loss') else None,
                'loss_history': history.get('loss', []),
                'val_loss_history': history.get('val_loss', [])
            },
            'model_config': model.get_config(),
            'optimizer_config': model.optimizer.get_config()
        }
    
    def compare_models(self) -> Dict[str, Any]:
        """
        Compare all trained models and provide recommendations
        
        Returns:
            Dictionary with model comparison and recommendations
        """
        if not self.trained_models:
            return {'error': 'No trained models available for comparison'}
        
        model_comparison = {}
        best_model = None
        best_score = float('inf')
        
        for model_name in self.trained_models:
            history = self.training_histories.get(model_name, {})
            
            if 'val_loss' in history:
                final_val_loss = history['val_loss'][-1]
                best_val_loss = min(history['val_loss'])
                
                model_comparison[model_name] = {
                    'final_val_loss': float(final_val_loss),
                    'best_val_loss': float(best_val_loss),
                    'parameter_count': int(self.trained_models[model_name].count_params()),
                    'epochs_trained': len(history['loss']),
                    'overfitting_ratio': float(final_val_loss / history['loss'][-1]) if history['loss'][-1] > 0 else 1.0
                }
                
                # Simple scoring: lower validation loss is better
                if final_val_loss < best_score:
                    best_score = final_val_loss
                    best_model = model_name
        
        return {
            'model_comparison': model_comparison,
            'recommendation': {
                'best_model': best_model,
                'best_val_loss': float(best_score),
                'comparison_criteria': 'final_validation_loss'
            },
            'summary': {
                'total_models': len(self.trained_models),
                'models_compared': len(model_comparison)
            }
        }


class TrainingLogger(tf.keras.callbacks.Callback):
    """
    Custom callback for enhanced training logging
    
    PURPOSE: Provides detailed training progress without overwhelming output
    """
    
    def __init__(self, logger, model_name: str, log_frequency: int = 10):
        super().__init__()
        self.logger = logger
        self.model_name = model_name
        self.log_frequency = log_frequency
        self.best_val_loss = float('inf')
    
    def on_train_begin(self, logs=None):
        self.logger.info(f"Training started for {self.model_name}")
    
    def on_epoch_end(self, epoch, logs=None):
        logs = logs or {}
        
        # Log every nth epoch or if validation loss improved
        val_loss = logs.get('val_loss', float('inf'))
        should_log = (epoch + 1) % self.log_frequency == 0 or val_loss < self.best_val_loss
        
        if should_log:
            train_loss = logs.get('loss', 0)
            train_mae = logs.get('mae', 0)
            val_mae = logs.get('val_mae', 0)
            
            if val_loss < self.best_val_loss:
                self.best_val_loss = val_loss
                improvement_msg = " (improved)"
            else:
                improvement_msg = ""
            
            self.logger.info(
                f"{self.model_name} - Epoch {epoch+1}: "
                f"Loss: {train_loss:.6f}, Val Loss: {val_loss:.6f}, "
                f"MAE: {train_mae:.6f}, Val MAE: {val_mae:.6f}{improvement_msg}"
            )
    
    def on_train_end(self, logs=None):
        self.logger.info(f"Training completed for {self.model_name} - Best Val Loss: {self.best_val_loss:.6f}")


class ModelEvaluator:
    """
    Comprehensive model evaluation utilities
    
    PURPOSE: Provides detailed model performance analysis beyond basic metrics
    """
    
    def __init__(self):
        self.logger = logging.getLogger('ModelEvaluator')
    
    def evaluate_model_performance(self, y_true: np.ndarray, y_pred: np.ndarray, 
                                 model_name: str = "model") -> Dict[str, Any]:
        """
        Comprehensive model performance evaluation
        
        Args:
            y_true: True values
            y_pred: Predicted values
            model_name: Name of the model being evaluated
            
        Returns:
            Dictionary with comprehensive performance metrics
        """
        try:
            # Basic regression metrics
            mse = float(np.mean((y_true - y_pred) ** 2))
            rmse = float(np.sqrt(mse))
            mae = float(np.mean(np.abs(y_true - y_pred)))
            mape = float(np.mean(np.abs((y_true - y_pred) / y_true)) * 100) if np.all(y_true != 0) else float('inf')
            
            # R² score
            ss_res = np.sum((y_true - y_pred) ** 2)
            ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
            r2 = float(1 - (ss_res / ss_tot)) if ss_tot != 0 else 0.0
            
            # Directional accuracy (for time series)
            y_true_diff = np.diff(y_true)
            y_pred_diff = np.diff(y_pred)
            directional_accuracy = float(np.mean(np.sign(y_true_diff) == np.sign(y_pred_diff))) if len(y_true_diff) > 0 else 0.0
            
            # Residual analysis
            residuals = y_true - y_pred
            residual_std = float(np.std(residuals))
            residual_mean = float(np.mean(residuals))
            
            # Prediction intervals (simple approach)
            prediction_std = float(np.std(y_pred))
            
            return {
                'model_name': model_name,
                'basic_metrics': {
                    'mse': mse,
                    'rmse': rmse,
                    'mae': mae,
                    'mape': mape,
                    'r2_score': r2
                },
                'advanced_metrics': {
                    'directional_accuracy': directional_accuracy,
                    'prediction_std': prediction_std,
                    'residual_mean': residual_mean,
                    'residual_std': residual_std
                },
                'data_info': {
                    'sample_size': len(y_true),
                    'true_mean': float(np.mean(y_true)),
                    'true_std': float(np.std(y_true)),
                    'pred_mean': float(np.mean(y_pred)),
                    'pred_std': float(np.std(y_pred))
                }
            }
            
        except Exception as e:
            return {
                'model_name': model_name,
                'error': f'Evaluation failed: {str(e)}'
            }


# Utility functions for data preprocessing
def prepare_market_data(price_series: pd.Series, 
                       add_features: bool = True) -> np.ndarray:
    """
    Prepare market data for deep learning models
    
    PURPOSE: Creates features and handles missing data for model training
    
    Args:
        price_series: Time series of prices
        add_features: Whether to add technical indicators as features
        
    Returns:
        Preprocessed data array ready for model training
    """
    try:
        # Basic preprocessing
        data = price_series.dropna().values
        
        if add_features and len(data) > 20:
            # Add simple technical features
            returns = np.diff(data) / data[:-1]
            
            # Simple moving averages
            ma_5 = pd.Series(data).rolling(5, min_periods=1).mean().values
            ma_10 = pd.Series(data).rolling(10, min_periods=1).mean().values
            
            # Volatility (rolling standard deviation of returns)
            volatility = pd.Series(returns).rolling(10, min_periods=1).std().fillna(0).values
            
            # Combine features (align lengths)
            min_length = min(len(data), len(ma_5), len(ma_10), len(volatility) + 1)
            
            features = np.column_stack([
                data[:min_length],
                ma_5[:min_length],
                ma_10[:min_length],
                np.concatenate([[0], volatility[:min_length-1]])  # Pad volatility
            ])
            
            return features
        else:
            return data.reshape(-1, 1)
            
    except Exception as e:
        logger.error(f"Data preparation failed: {str(e)}")
        return price_series.dropna().values.reshape(-1, 1)


def validate_tensorflow_installation() -> Dict[str, Any]:
    """
    Validate TensorFlow installation and GPU availability
    
    Returns:
        Dictionary with installation status and recommendations
    """
    validation_result = {
        'tensorflow_available': TENSORFLOW_AVAILABLE,
        'version': None,
        'gpu_available': False,
        'gpu_devices': [],
        'recommendations': []
    }
    
    if TENSORFLOW_AVAILABLE:
        validation_result['version'] = tf.__version__
        
        # Check GPU availability
        try:
            physical_devices = tf.config.list_physical_devices('GPU')
            validation_result['gpu_available'] = len(physical_devices) > 0
            validation_result['gpu_devices'] = [device.name for device in physical_devices]
            
            if not validation_result['gpu_available']:
                validation_result['recommendations'].append(
                    "Consider installing GPU version of TensorFlow for faster training"
                )
        except Exception:
            validation_result['recommendations'].append(
                "GPU detection failed - check TensorFlow-GPU installation"
            )
    else:
        validation_result['recommendations'].append(
            "Install TensorFlow: pip install tensorflow"
        )
    
    return validation_result


# Export main classes and functions
__all__ = [
    'MarketModelBuilder', 
    'TrainingLogger', 
    'ModelEvaluator',
    'prepare_market_data',
    'validate_tensorflow_installation'
]