#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PyTorch LSTM Architecture for Sequence-to-Point Price Forecasting
------------------------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/LSTM Price Forecaster/model.py
Input      : Tensor of shape (Batch, 60, 5) [open, high, low, close, volume]
Output     : Tensor of shape (Batch, 2) [P_t+15m, P_t+60m]
"""

import torch
import torch.nn as nn


class LSTMPriceForecaster(nn.Module):
    """
    Two-stage Stacked LSTM with Dropout Regularization and Dense Projection.
    
    Architecture:
      Input (B, 60, 5)
        │
      LSTM Layer 1 (hidden_size=64, return_sequences=True)
        │
      Dropout (p=0.20)
        │
      LSTM Layer 2 (hidden_size=32, return_sequences=False -> last step)
        │
      Dense 1 (32 -> 16) + ReLU
        │
      Dropout (p=0.10)
        │
      Output Dense 2 (16 -> 2) -> [y_t+15m, y_t+60m]
    """
    def __init__(
        self,
        input_size: int = 7,
        hidden_size_1: int = 64,
        hidden_size_2: int = 32,
        dense_hidden: int = 16,
        output_size: int = 2,
        dropout: float = 0.20
    ):
        super(LSTMPriceForecaster, self).__init__()
        
        self.input_size = input_size
        self.hidden_size_1 = hidden_size_1
        self.hidden_size_2 = hidden_size_2
        self.output_size = output_size
        
        # Layer 1: First LSTM
        self.lstm1 = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size_1,
            num_layers=1,
            batch_first=True
        )
        self.dropout1 = nn.Dropout(p=dropout)
        
        # Layer 2: Second LSTM
        self.lstm2 = nn.LSTM(
            input_size=hidden_size_1,
            hidden_size=hidden_size_2,
            num_layers=1,
            batch_first=True
        )
        self.dropout2 = nn.Dropout(p=dropout * 0.5)
        
        # Temporal Attention Mechanism (captures non-local volatility signals across 60 bars)
        self.attn_dense = nn.Linear(hidden_size_2, 1)

        # Layer 3: Fully Connected Head
        self.fc1 = nn.Linear(hidden_size_2, dense_hidden)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(dense_hidden, output_size)
        
        self._init_weights()

    def _init_weights(self):
        """Xavier / Kaiming weight initialization for stability and faster convergence."""
        for name, param in self.lstm1.named_parameters():
            if "weight_ih" in name:
                nn.init.xavier_uniform_(param.data)
            elif "weight_hh" in name:
                nn.init.orthogonal_(param.data)
            elif "bias" in name:
                param.data.fill_(0)
                # Forget gate bias init to 1.0 (LSTM best practice)
                n = param.size(0)
                param.data[(n // 4):(n // 2)].fill_(1.0)
                
        for name, param in self.lstm2.named_parameters():
            if "weight_ih" in name:
                nn.init.xavier_uniform_(param.data)
            elif "weight_hh" in name:
                nn.init.orthogonal_(param.data)
            elif "bias" in name:
                param.data.fill_(0)
                n = param.size(0)
                param.data[(n // 4):(n // 2)].fill_(1.0)
                
        nn.init.xavier_uniform_(self.attn_dense.weight)
        nn.init.zeros_(self.attn_dense.bias)
        nn.init.kaiming_normal_(self.fc1.weight, nonlinearity="relu")
        nn.init.zeros_(self.fc1.bias)
        nn.init.xavier_uniform_(self.fc2.weight)
        nn.init.zeros_(self.fc2.bias)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """
        Forward computation compatible with TorchScript JIT tracing.
        Args:
            x: Input tensor of shape (Batch, Sequence_Length, Features), e.g. (B, 60, 7)
        Returns:
            out: Predicted price vector of shape (Batch, 2)
        """
        # lstm1 output shape: (B, 60, 64)
        out1, _ = self.lstm1(x)
        drop1 = self.dropout1(out1)
        
        # lstm2 output shape: (B, 60, 32)
        out2, _ = self.lstm2(drop1)
        drop2 = self.dropout2(out2)
        
        # Temporal Attention Pooling across all 60 time steps
        attn_scores = self.attn_dense(drop2) # (B, 60, 1)
        attn_weights = torch.softmax(attn_scores, dim=1) # (B, 60, 1)
        context_vector = torch.sum(attn_weights * drop2, dim=1) # (B, 32)
        
        # Residual fusion: Last step + Attention context
        last_step = drop2[:, -1, :] # (B, 32)
        representation = last_step + context_vector # (B, 32)
        
        # Projection head
        feat = self.relu(self.fc1(representation))
        preds = self.fc2(feat)
        return preds
