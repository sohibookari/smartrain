from logging import Logger

import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.svm import SVC

import smartrain.context as ctx
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.model_selection import cross_val_score
from imblearn.over_sampling import SMOTE
from smartrain.utils import series_to_ndarray


class Trainer:
    def __init__(self, sample_set, kwargs):

        self.logger: Logger = ctx.get('logger')
        self.sample_set = sample_set
        self.label_col = kwargs.get('label_col')
        self.useless_cols = kwargs.get('useless_cols')
        self.y_resampled = None
        self.x_resampled = None
        self.y_test = None
        self.y_train = None
        self.x_test = None
        self.x_train = None
        self.sorted_null_col_index = None
        self.null_col_list = None

        self.label_set = self.sample_set[self.label_col]
        self.sample_set.drop(columns=self.label_col, inplace=True)

        for col in self.useless_cols:
            self.sample_set.drop(columns=col, inplace=True)

    def prepare(self):
        # fill check in col
        col = 'lessonparticipants'
        cur_col = self.sample_set.loc[:, col]
        imp_mean = SimpleImputer(missing_values=np.nan, strategy='constant', fill_value=0)
        self.sample_set.loc[:, col] = imp_mean.fit_transform(series_to_ndarray(cur_col))

        self.logger.info(self.sample_set.info())
        self.null_col_list = self.sample_set.isnull().sum().sort_values()
        self.sorted_null_col_index = np.argsort(self.null_col_list[self.null_col_list.values != 0]).index

        return self

    def fill(self, method):
        for col in self.sorted_null_col_index:
            imp_mean = SimpleImputer(missing_values=np.nan, strategy=method)
            cur_col = self.sample_set.loc[:, col]
            self.sample_set.loc[:, col] = imp_mean.fit_transform(series_to_ndarray(cur_col))
        return self

    def rtm_fill(self):
        for i in self.sorted_null_col_index:
            df = self.sample_set
            cur_col = df.loc[:, i]

            y_train_null = cur_col[cur_col.notna()]
            y_test_null = cur_col[cur_col.isna()]
            x_train_null = df.loc[y_train_null.index, self.null_col_list[self.null_col_list == 0].index]
            x_test_null = df.loc[y_test_null.index, self.null_col_list[self.null_col_list == 0].index]

            rfc = RandomForestRegressor()
            rfc.fit(x_train_null, y_train_null)
            y_predict = rfc.predict(x_test_null)

            self.sample_set.loc[self.sample_set.loc[:, i].isnull(), i] = y_predict
            # update not null col.
            self.null_col_list = self.sample_set.isnull().sum().sort_values()
        return self

    def resample(self):
        smt = SMOTE(random_state=0)
        self.x_resampled, self.y_resampled = smt.fit_resample(self.sample_set, self.label_set)
        self.x_train, self.x_test, self.y_train, self.y_test = train_test_split(self.x_resampled, self.y_resampled,
                                                                                test_size=0.20, random_state=0)
        return self

    def optimization(self):
        scores = []
        for i in range(1, 51, 1):
            rfc = RandomForestClassifier(n_estimators=170,
                                         max_depth=26,
                                         random_state=90)
            score = cross_val_score(rfc, self.x_train, self.y_train).mean()
            scores.append(score)

        print(max(scores), (scores.index(max(scores))))
        plt.figure(figsize=[20, 5])
        # plt.plot(range(1, 201, 10), scores)
        sns.lineplot(x=list(range(1, 51, 1)), y=scores)
        plt.show()

    def val(self):
        models = [
            RandomForestClassifier(n_estimators=170, max_depth=26),
            LinearRegression(fit_intercept=True, normalize=False,
                             copy_X=True, n_jobs=1),
            LogisticRegression(),
            SVC(C=1.0, kernel='rbf', gamma='auto'),
            KNeighborsClassifier(n_neighbors=5),
            MLPClassifier(activation='relu', solver='adam', alpha=0.0001)
        ]

        for m in models:
            score = cross_val_score(m, X=self.x_resampled, y=self.y_resampled)
            self.logger.info("%s: %f" % (getattr(m, '__str__')(), score.mean()))
        return self

    def train(self):
        self.logger.info('start fit model.')
        rfc = RandomForestClassifier(n_estimators=191, max_depth=26, random_state=1, min_samples_leaf=1)
        rfc = self._train(rfc)
        self._score(rfc)

    def _train(self, model):
        model.fit(self.x_train, self.y_train)
        return model

    def _score(self, model):
        self.logger.info("%s: %f" % (model.__str__(), model.score(self.x_test, self.y_test)))
        return self
