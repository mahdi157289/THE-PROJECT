"""
Lightweight ML Models for Market Analysis (scikit-learn)
=======================================================

PURPOSE:
- Replace DL (TF/Keras) temporarily with scikit-learn MLPRegressor
- Keep the same public API used by the pipeline:
  - MarketModelBuilder
    - _create_sequences(data)
    - build_cnn_lstm(input_shape)
    - build_transformer(input_shape)
    - train_model(model, X, y, epochs=..., model_name=...)
    - predict_with_model(model_name, X_test)
    - get_model_summary(model_name)
    - compare_models()
- Provide simple, robust predictions for Power BI via diamond_predictions
"""

from __future__ import annotations
import numpy as np
import pandas as pd
import logging
from typing import Tuple, Dict, Any, Optional

from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error

logger = logging.getLogger('DeepModels')


class SklearnModelWrapper:
    """Minimal wrapper to mimic Keras-like API parts used in the pipeline."""
    def __init__(self, model_name: str, hidden_layers: Tuple[int, ...], input_shape: Tuple[int, int]):
        self.model_name = model_name
        self.hidden_layer_sizes = hidden_layers
        self.input_shape = input_shape  # (timesteps, features)

    def count_params(self) -> int:
        # Approximate parameter count for an MLP: sum((in+1)*h) + (last+1)*1
        timesteps, features = self.input_shape
        input_dim = timesteps * features
        params = 0
        prev = input_dim
        for h in self.hidden_layer_sizes:
            params += (prev + 1) * h  # weights + biases
            prev = h
        params += (prev + 1) * 1
        return int(params)


class MarketModelBuilder:
    def __init__(self, lookback_window: int = 60, validation_split: float = 0.2):
        self.lookback_window = lookback_window
        self.validation_split = validation_split
        self.logger = logger
        self._configure_logger()
        self.trained_models: Dict[str, MLPRegressor] = {}
        self.training_histories: Dict[str, Dict[str, list]] = {}

    def _configure_logger(self):
        if not self.logger.handlers:
            h = logging.StreamHandler()
            f = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            h.setFormatter(f)
            self.logger.addHandler(h)
            self.logger.setLevel(logging.INFO)

    def _create_sequences(self, data: np.ndarray, target_column: int = 0) -> Tuple[np.ndarray, np.ndarray]:
        if len(data.shape) == 1:
            data = data.reshape(-1, 1)
        X, y = [], []
        for i in range(len(data) - self.lookback_window):
            X.append(data[i:i + self.lookback_window])
            y.append(data[i + self.lookback_window, target_column])
        X = np.array(X)
        y = np.array(y)
        self.logger.debug(f"Created {len(X)} sequences with shape {X.shape}")
        return X, y

    # Keep the builder names for compatibility; they return a wrapper describing the MLP layout
    def build_cnn_lstm(self, input_shape: Tuple[int, int],
                       conv_filters: int = 64,
                       lstm_units: int = 100,
                       dropout_rate: float = 0.2) -> SklearnModelWrapper:
        # Map to a 2-layer MLP
        return SklearnModelWrapper("cnn_lstm", hidden_layers=(64, 64), input_shape=input_shape)

    def build_transformer(self, input_shape: Tuple[int, int],
                          num_heads: int = 4,
                          ff_dim: int = 64,
                          num_transformer_blocks: int = 2,
                          dropout_rate: float = 0.1) -> SklearnModelWrapper:
        # Map to a slightly different MLP size
        return SklearnModelWrapper("transformer", hidden_layers=(128, 64), input_shape=input_shape)

    def train_model(self, model: SklearnModelWrapper,
                    X_train: np.ndarray,
                    y_train: np.ndarray,
                    epochs: int = 50,
                    batch_size: int = 32,
                    model_name: str = "model") -> Dict[str, Any]:
        """
        Train an MLPRegressor using flattened sequences.
        Returns a dict with final_metrics similar to the previous DL version.
        """
        self.logger.info(f"Starting training for {model_name}")
        # Flatten timesteps*features → a single feature vector per sample
        X_train_flat = X_train.reshape(X_train.shape[0], -1)

        # Simple holdout for "validation"
        split_idx = int(len(X_train_flat) * (1 - self.validation_split))
        X_tr, y_tr = X_train_flat[:split_idx], y_train[:split_idx]
        X_val, y_val = X_train_flat[split_idx:], y_train[split_idx:]

        mlp = MLPRegressor(
            hidden_layer_sizes=model.hidden_layer_sizes,
            activation='relu',
            solver='adam',
            random_state=42,
            max_iter=max(200, epochs * 20)  # let it converge a bit
        )
        mlp.fit(X_tr, y_tr)

        # Store trained model
        self.trained_models[model_name] = mlp

        # Metrics
        y_tr_pred = mlp.predict(X_tr)
        train_mse = mean_squared_error(y_tr, y_tr_pred)
        train_mae = mean_absolute_error(y_tr, y_tr_pred)

        if len(X_val) > 0:
            y_val_pred = mlp.predict(X_val)
            val_mse = mean_squared_error(y_val, y_val_pred)
            val_mae = mean_absolute_error(y_val, y_val_pred)
        else:
            y_val_pred = y_tr_pred
            val_mse = train_mse
            val_mae = train_mae

        final_results = {
            'model_name': model_name,
            'training_completed': True,
            'final_metrics': {
                'train_loss': float(train_mse),
                'val_loss': float(val_mse),
                'train_mae': float(train_mae),
                'val_mae': float(val_mae),
            },
            'model_info': {
                'total_parameters': model.count_params(),
                'model_architecture': 'MLPRegressor',
                'input_shape': list(X_train.shape[1:]),
                'output_shape': [1]
            },
            'training_config': {
                'epochs': epochs,
                'batch_size': batch_size,
                'validation_split': self.validation_split,
                'optimizer': 'adam',
                'learning_rate': 'adaptive'
            }
        }
        self.logger.info(
            f"Training completed for {model_name}: "
            f"Val MSE: {val_mse:.6f}, Val MAE: {val_mae:.6f}"
        )
        return final_results

    def predict_with_model(self, model_name: str, X_test: np.ndarray) -> Dict[str, Any]:
        if model_name not in self.trained_models:
            return {'error': f'Model {model_name} not found in trained models'}
        mlp = self.trained_models[model_name]
        X_test_flat = X_test.reshape(X_test.shape[0], -1)
        preds = mlp.predict(X_test_flat)
        preds = preds.astype(float)
        return {
            'model_name': model_name,
            'predictions': preds.tolist(),
            'prediction_stats': {
                'mean': float(np.mean(preds)),
                'std': float(np.std(preds)),
                'min': float(np.min(preds)),
                'max': float(np.max(preds)),
                'count': int(preds.shape[0])
            }
        }

    def get_model_summary(self, model_name: str) -> Dict[str, Any]:
        if model_name not in self.trained_models:
            return {'error': f'Model {model_name} not found'}
        mlp = self.trained_models[model_name]
        return {
            'model_name': model_name,
            'architecture_summary': 'Scikit-learn MLPRegressor',
            'parameter_count': getattr(mlp, 'coefs_', None) and int(sum(w.size for w in mlp.coefs_)) or None,
            'training_history': {},
            'model_config': {
                'hidden_layer_sizes': getattr(mlp, 'hidden_layer_sizes', None),
                'activation': getattr(mlp, 'activation', None),
                'solver': getattr(mlp, 'solver', None),
                'random_state': getattr(mlp, 'random_state', None)
            }
        }

    def compare_models(self) -> Dict[str, Any]:
        if not self.trained_models:
            return {'error': 'No trained models available for comparison'}
        # No explicit per-epoch history; compare by simple train score
        comparison = {}
        best_model = None
        best_score = float('inf')
        for name, mlp in self.trained_models.items():
            # Use .loss_ if available, else 0.0 (scikit may expose loss_ after training)
            score = getattr(mlp, 'loss_', 0.0) or 0.0
            comparison[name] = {
                'final_val_loss': float(score),
                'epochs_trained': None
            }
            if score < best_score:
                best_score = score
                best_model = name
        return {
            'model_comparison': comparison,
            'recommendation': {
                'best_model': best_model,
                'best_val_loss': float(best_score),
                'comparison_criteria': 'MLPRegressor final_loss'
            },
            'summary': {
                'total_models': len(self.trained_models),
                'models_compared': len(comparison)
            }
        }


def prepare_market_data(price_series: pd.Series,
                        add_features: bool = True) -> np.ndarray:
    """
    Prepare market data for MLP models (simple features).
    """
    try:
        data = price_series.dropna().values
        if add_features and len(data) > 20:
            returns = np.diff(data) / data[:-1]
            ma_5 = pd.Series(data).rolling(5, min_periods=1).mean().values
            ma_10 = pd.Series(data).rolling(10, min_periods=1).mean().values
            volatility = pd.Series(returns).rolling(10, min_periods=1).std().fillna(0).values
            min_length = min(len(data), len(ma_5), len(ma_10), len(volatility) + 1)
            features = np.column_stack([
                data[:min_length],
                ma_5[:min_length],
                ma_10[:min_length],
                np.concatenate([[0], volatility[:min_length - 1]])
            ])
            return features
        else:
            return data.reshape(-1, 1)
    except Exception as e:
        logger.error(f"Data preparation failed: {str(e)}")
        return price_series.dropna().values.reshape(-1, 1)


__all__ = [
    'MarketModelBuilder',
    'prepare_market_data'
]