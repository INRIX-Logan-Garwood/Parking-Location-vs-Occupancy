'''
File to build classes for the ML models
'''

import numpy as np
import pandas as pd
import torch

class Model:
    def __init__(self, input_dim):
        self.model = self.initialize_model(input_dim)
        self.loss_fn = torch.nn.MSELoss()
        self.optimizer = torch.optim.Adam(self.model.parameters())
        self.device = torch.device('cpu') # torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    def initialize_model(self, input_dim):
        input_dim = input_dim
        output_dim = 1
        hidden_layer = 10
        return torch.nn.Sequential(
            torch.nn.Linear(input_dim, hidden_layer),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_layer, output_dim)
        ).to(self.device)

    def fit(self, X, y, epochs=100, batch_size=32):
        X = torch.tensor(X, dtype=torch.float32).to(self.device)
        y = torch.tensor(y, dtype=torch.float32).to(self.device)

        for epoch in range(epochs):
            self.model.train()
            for i in range(0, X.shape[0], batch_size):
                self.optimizer.zero_grad()
                y_pred = self.model(X[i:i+batch_size])
                loss = self.loss_fn(y_pred, y[i:i+batch_size])
                loss.backward()
                self.optimizer.step()

    def predict(self, X):
        self.model.eval()
        with torch.no_grad():
            return self.model(torch.tensor(X, dtype=torch.float32).to(self.device)).cpu().numpy()

    def save(self, path):
        torch.save(self.model.state_dict(), path)

    def load(self, path):
        self.model.load_state_dict(torch.load(path))