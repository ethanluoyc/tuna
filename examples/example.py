from tuna import create_experiments, grid_search


def train(config, reporter=None):
    print(config)

if __name__ == '__main__':
    exps = {
        'foo': {"run": "run",
                "entry": train,
                "stop": {"mean_accuracy": 100},
                "config": {
                    "alpha": grid_search([0.2, 0.4, 0.6]),
                    "beta": grid_search([1, 2]),
                }}
    }
    create_experiments(exps)
