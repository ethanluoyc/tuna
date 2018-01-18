from tuna.variant_generator import make_parser
import sys


if __name__ == '__main__':
    parser = make_parser()
    args = parser.parse_known_args()
    print(args)