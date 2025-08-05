"""Deep learning models for market analysis"""
import numpy as np
import pandas as pd
import tensorflow as tf
import logging
from tensorflow.keras.models import Model, Sequential
from tensorflow.keras.layers import (Conv1D, LSTM, Dense, Flatten, Input, 
                                    Lambda, LayerNormalization, MultiHeadAttention)
from tensorflow.keras.optimizers import Adam

logger = logging.getLogger('DeepModels')

class MarketModelBuilder:
    def __init__(self, lookback_window=60):
        self.lookback_window = lookback_window
        self.logger = logger
    
    def _create_sequences(self, data: np.ndarray) -> tuple:
        """Create input-output sequences from time series"""
        X, y = [], []
        for i in range(len(data) - self.lookback_window):
            X.append(data[i:i+self.lookback_window])
            y.append(data[i+self.lookback_window])
        return np.array(X), np.array(y)
    
    def build_cnn_lstm(self, input_shape: tuple) -> Model:
        """CNN-LSTM hybrid model"""
        model = Sequential([
            Input(shape=input_shape),
            Conv1D(64, 3, activation='relu'),
            Conv1D(64, 3, activation='relu'),
            LSTM(100, return_sequences=True),
            LSTM(100),
            Dense(1)
        ])
        model.compile(optimizer=Adam(0.001), loss='mse')
        self.logger.info("Built CNN-LSTM model")
        return model
    
    def build_transformer(self, input_shape: tuple, num_heads=4, ff_dim=64) -> Model:
        """Transformer model for time series"""
        inputs = Input(shape=input_shape)
        
        # Positional encoding (simplified)
        x = LayerNormalization(epsilon=1e-6)(inputs)
        
        # Transformer block
        for _ in range(2):
            # Multi-head self-attention
            attn_output = MultiHeadAttention(num_heads=num_heads, key_dim=input_shape[-1])(x, x)
            x = LayerNormalization(epsilon=1e-6)(x + attn_output)
            
            # Feed-forward network
            ffn = Sequential([
                Dense(ff_dim, activation='relu'),
                Dense(input_shape[-1])
            ])
            ffn_output = ffn(x)
            x = LayerNormalization(epsilon=1e-6)(x + ffn_output)
        
        # Output
        x = Flatten()(x)
        outputs = Dense(1)(x)
        
        model = Model(inputs=inputs, outputs=outputs)
        model.compile(optimizer=Adam(0.001), loss='mse')
        self.logger.info("Built Transformer model")
        return model
    
    def train_model(self, model: Model, X_train: np.ndarray, y_train: np.ndarray, 
                   epochs=50, batch_size=32) -> Model:
        """Train model with logging callback"""
        class LoggingCallback(tf.keras.callbacks.Callback):
            def on_epoch_end(self, epoch, logs=None):
                if (epoch + 1) % 10 == 0:
                    logger.info(f"Epoch {epoch+1}/{epochs} - loss: {logs['loss']:.4f}")
        
        history = model.fit(
            X_train, y_train,
            epochs=epochs,
            batch_size=batch_size,
            verbose=0,
            callbacks=[LoggingCallback()]
        )
        self.logger.info(f"Training completed - final loss: {history.history['loss'][-1]:.4f}")
        return model