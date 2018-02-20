from tuna import create_experiments, grid_search


def train(ctx):
    print(ctx.config)

if __name__ == '__main__':
    exps = {
        'foo': {"run": "run",
                "entry": train,
                "config": {
                    "alpha": grid_search([0.2, 0.4, 0.6]),
                    "beta": grid_search([1, 2]),
                }}
    }
    create_experiments(exps)
