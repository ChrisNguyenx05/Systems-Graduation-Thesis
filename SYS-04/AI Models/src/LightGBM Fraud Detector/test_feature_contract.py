import unittest

import pandas as pd

from feature_contract import FEATURE_NAMES, FraudFeatureTransformer, validate_serving_payload


class FeatureContractTests(unittest.TestCase):
    def test_transformer_keeps_the_exact_feature_order(self):
        frame = pd.DataFrame([{name: 1 if name not in {"ProductCD", "card4", "card6", "P_emaildomain", "R_emaildomain"} else "x" for name in FEATURE_NAMES}])
        transformed = FraudFeatureTransformer().fit(frame).transform(frame)
        self.assertEqual(list(transformed.columns), FEATURE_NAMES)

    def test_serving_payload_rejects_missing_fields(self):
        with self.assertRaises(ValueError):
            validate_serving_payload({"TransactionAmt": 1.0})


if __name__ == "__main__":
    unittest.main()
