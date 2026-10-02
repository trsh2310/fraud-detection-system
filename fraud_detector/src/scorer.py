import pandas as pd
import logging
from catboost import CatBoostClassifier, Pool

# Настройка логгера
logger = logging.getLogger(__name__)

logger.info('Importing pretrained model...')

# Import model
model = CatBoostClassifier()
model.load_model('./models/my_catboost.cbm')

# Define optimal threshold
model_th = 0.98
logger.info('Pretrained model imported successfully...')


def make_pred(dt, source_info="kafka"):

    # Меняем формат категориальных фичей на string перед скорингом
    expected_categorical = ['hour',
                            'year',
                            'month',
                            'day_of_month',
                            'day_of_week',
                            'gender_cat',
                            'merch_cat',
                            'cat_id_cat',
                            'one_city_cat',
                            'us_state_cat',
                            'jobs_cat']
    for col in expected_categorical:
        if col in dt.columns:
            dt[col] = dt[col].astype(str)

    # Calculate score
    # Match the saved model schema even if the incoming row contains extra fields.
    features = dt.loc[:, model.feature_names_]
    scores = model.predict_proba(Pool(features, cat_features=model.get_cat_feature_indices()))[:, 1]
    submission = pd.DataFrame({
        'score': scores,
        'fraud_flag': (scores > model_th).astype(int)
    })
    logger.info(f'Prediction complete for data from {source_info}')

    # Return proba for positive class
    return submission
