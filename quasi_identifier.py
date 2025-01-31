class QuasiIdentifier:
    def __init__(self, dataset, column_name):
        self.dataset = dataset
        self.column_name = column_name
        self.min_value = dataset[column_name].min()
        self.max_value = dataset[column_name].max()

    def get_range(self):
        return self.max_value - self.min_value + 1
