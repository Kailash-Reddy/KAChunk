import pandas as pd

class Metadata:
    def __init__(self, metadata: dict, data: pd.DataFrame):
        self.metadata = metadata
        self.data = data

    def get_numerical_columns(self):
        numerical_columns = [
            col for col, dtype in self.data.dtypes.items()
            if pd.api.types.is_numeric_dtype(dtype)
        ]
        return numerical_columns

    def get_quasi_identifiers(self, chosen_columns):
        numerical_columns = self.get_numerical_columns()
        return [col for col in chosen_columns if col in numerical_columns]

