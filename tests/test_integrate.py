import tempfile
import unittest
from tuna.logger import UnifiedLogger
from tuna.result import TrainingResult
from tensorboardX import SummaryWriter
def example_train_fn(ctx):
    pass


class TestRun(unittest.TestCase):
    def test_stuff(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            print(tmpdir)


def main():
    pass

if __name__ == "__main__":
    # unittest.main()
    logger = UnifiedLogger({}, './')
    for i in range(10):
        logger.on_result(TrainingResult(timesteps_this_iter=i,
                                        mean_loss=i,
                                        timesteps_total=i))