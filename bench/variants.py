"""List source variants without importing DuckDB or generating data."""

import argparse

if __package__:
    from .ssb_variants import VARIANTS
else:
    from ssb_variants import VARIANTS


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--list", action="store_true", required=True, help="list supported variants"
    )
    parser.parse_args(argv)
    print("\n".join(VARIANTS))


if __name__ == "__main__":
    main()
