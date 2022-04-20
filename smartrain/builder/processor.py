import numpy as np
import pandas as pd
import sklearn

from smartrain.utils import series_to_ndarray


class Processor:
    def __init__(self, data):
        self.data = data

    def get_ratio(self, target, cola, colb):
        self.data[target] = self.data[cola] / self.data[colb]
        return self

    def sum_by_group(self, target, *cols):
        self.data = self.data.groupby(by=list(cols), axis=0)[target].sum().reset_index()
        return self

    def count_by_group(self, target, *cols):
        self.data = self.data.groupby(by=list(cols), axis=0)[target].count().reset_index()
        return self

    def normalize(self, target):
        from sklearn.preprocessing import MinMaxScaler
        scalar = MinMaxScaler()
        self.data[target] = scalar.fit_transform(series_to_ndarray(self.data[target]))
        return self

    def normalize_by_zscore(self, target):
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        self.data[target] = scaler.fit_transform(series_to_ndarray(self.data[target]))
        return self

    def cluster(self, target, k):
        from sklearn.cluster import KMeans
        cluster = KMeans(n_clusters=k, random_state=0)
        self.data['cluster'] = cluster.fit_predict(series_to_ndarray(self.data[target]))
        return self

    def get_data(self) -> pd.DataFrame:
        return self.data

    def get_column(self, col) -> pd.Series:
        return self.data[col]


class TimeProcessor(Processor):
    def __init__(self, data: pd.Series):
        super().__init__(data)

    def scale_by_zscore(self):
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        if isinstance(self.data, pd.Series):
            self.data = series_to_ndarray(self.data)
        self.data = scaler.fit_transform(self.data)
        return self

    def evaluate_by_LocalOutlierFactor(self):
        from sklearn.neighbors import LocalOutlierFactor
        clf = LocalOutlierFactor(n_neighbors=10)
        y_pred = clf.fit_predict(self.data)
        return y_pred

    def evaluate_by_EllipticEnvelope(self):
        from sklearn.covariance import EllipticEnvelope
        cov = EllipticEnvelope(random_state=0)
        y_pred = cov.fit_predict(self.data)
        return y_pred
