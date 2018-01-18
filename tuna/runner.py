#!/usr/bin/env python

from tuna.variant_generator import make_parser
import os
import json
import logging

_TUNA_STATE = "tuna_state.json"
_logger = logging.getLogger(__name__)


class TunaRunner(object):
    def __init__(self, d, trainable, config):
        self.trainable = trainable
        self.config = config
        self._state = {}
        self.d = d

    def finalize(self):
        os.remove(os.path.join(self.d, _TUNA_STATE))

    def run(self):
        if os.path.exists(os.path.join(self.d, _TUNA_STATE)):
            self._state = json.load(os.path.join(self.d, _TUNA_STATE))
            ckpt_path = self._state.get('checkpoint_path', None)
            if ckpt_path:
                self.trainable.restore(ckpt_path)
        else:
            self._update_state()

        for steps in range(2):
            train_result = self.trainable.train()
            ckpt_path = self.trainable.save()
            self._state['checkpoint_path'] = ckpt_path

    def _update_state(self):
        with open(os.path.join(self.d, _TUNA_STATE), 'w') as f:
            json.dump(self._state, f)


if __name__ == '__main__':
    parser = make_parser()
    args = parser.parse_known_args()
    print(args)